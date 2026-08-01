from __future__ import annotations

from eurostream.governance.pii import PIIClassifier, classify_value, hash_pii


def test_email_classified():
    assert "EMAIL" in classify_value("ada.lovelace@eurocart.eu")


def test_iban_classified():
    assert "IBAN" in classify_value("DE89370400440532013000")


def test_ipv4_classified():
    assert "IP_ADDRESS" in classify_value("192.168.1.10")


def test_phone_classified():
    assert "PHONE" in classify_value("+4915112345678")


def test_plain_text_not_classified():
    assert classify_value("hello world") == []


def test_hex_uuid_rejected_as_iban():
    """A UUID must not trip the IBAN recognizer — the mod-97 checksum rejects
    look-alikes that naive country-prefixed regex would accept."""
    assert classify_value("550e8400-e29b-41d4-a716-446655440000") == []


def test_manifest_builds_and_detects_unregistered(tmp_path):
    classifier = PIIClassifier(tmp_path / "manifest.json")
    rows = [
        {"_table": "bronze.orders", "customer_id": "cust_1", "email": "a@b.de", "amount": 10.0},
        {"_table": "bronze.orders", "customer_id": "cust_2", "email": "c@d.fr", "amount": 20.0},
    ]
    classifier.build_from_rows(rows)
    classifier.save()
    assert classifier.load()["bronze.orders"]["email"] == ["EMAIL"]


def test_gate_detects_new_unregistered_pii(tmp_path):
    classifier = PIIClassifier(tmp_path / "manifest.json")
    classifier.load()
    rows = [{"_table": "bronze.orders", "iban": "DE89370400440532013000"}]
    findings = classifier.detect_unregistered(rows)
    assert findings and findings[0]["column"] == "iban"


def test_hash_is_deterministic_and_salted():
    a = hash_pii("value", salt="s1")
    b = hash_pii("value", salt="s1")
    c = hash_pii("value", salt="s2")
    assert a == b and a != c and a != "value"
