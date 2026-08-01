# EuroStream EU-region infrastructure.
# Every provider is pinned to an EU region — see docs/adr/0001-eu-region-choice.md.
# This file is the residency proof: no resource is created outside the EU.

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Pinned to Frankfurt (eu-central-1) — EU residency, not a default.
provider "aws" {
  region = "eu-central-1"
}

variable "project" {
  default = "eurostream"
}

variable "environment" {
  default = "prod"
}

locals {
  name = "${var.project}-${var.environment}"
}

# Object lake — region enforced by the provider block above.
resource "aws_s3_bucket" "lake" {
  bucket        = "${local.name}-lake"
  force_destroy = false
}

# Production hygiene: this bucket is private, full stop.
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket = aws_s3_bucket.lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# IAM role assumed by pipeline workers (streaming + batch + erasure worker).
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipeline" {
  name               = "${local.name}-pipeline"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "pipeline_lake" {
  name = "${local.name}-pipeline-lake"
  role = aws_iam_role.pipeline.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.lake.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.lake.arn
      },
    ]
  })
}

# MSK (Kafka) in the same EU region; the broker topics mirror the local bus
# topics. Kept declarative as the deployment target for the bus adapter.
resource "aws_msk_cluster" "events" {
  cluster_name           = "${local.name}-events"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
    }
  }
}

resource "aws_security_group" "msk" {
  name   = "${local.name}-msk"
  vpc_id = var.vpc_id

  # TLS client traffic (9094 is MSK's TLS listener) from the allowed CIDRs —
  # typically the private subnets where pipeline workers run.
  ingress {
    description = "TLS client access to Kafka brokers"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = var.allowed_client_cidrs
  }

  egress {
    description = "Allow broker responses"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

variable "vpc_id" {
  type        = string
  description = "VPC hosting the MSK cluster (private subnets recommended)."
}

variable "allowed_client_cidrs" {
  type        = list(string)
  description = "CIDRs allowed to reach MSK on the TLS port (worker subnets)."
  default     = ["10.0.0.0/16"]
}

output "bootstrap_brokers_tls" {
  description = "Feed this into EUROSTREAM_KAFKA_BOOTSTRAP_SERVERS."
  value       = aws_msk_cluster.events.bootstrap_brokers_tls
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

variable "subnet_ids" {
  type    = list(string)
  default = []
}
