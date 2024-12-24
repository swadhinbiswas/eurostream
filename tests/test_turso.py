from __future__ import annotations

from eurostream.turso import (
    TursoClient,
    _parse_turso_cell,
    _rewrite_sql_for_turso,
    _to_turso_arg,
)


def test_turso_sql_rewrite():
    sql = "SELECT count(*) FROM bronze.orders WHERE customer_id = ?"
    rewritten = _rewrite_sql_for_turso(sql)
    assert '"bronze.orders"' in rewritten

    sql_multi = (
        "SELECT * FROM gold.customer_360 c JOIN silver.orders o ON c.customer_id = o.customer_id"
    )
    rewritten_multi = _rewrite_sql_for_turso(sql_multi)
    assert '"gold.customer_360"' in rewritten_multi
    assert '"silver.orders"' in rewritten_multi


def test_turso_arg_serialization():
    assert _to_turso_arg(None) == {"type": "null"}
    assert _to_turso_arg(True) == {"type": "integer", "value": "1"}
    assert _to_turso_arg(False) == {"type": "integer", "value": "0"}
    assert _to_turso_arg(42) == {"type": "integer", "value": "42"}
    assert _to_turso_arg(3.14) == {"type": "float", "value": 3.14}
    assert _to_turso_arg("test") == {"type": "text", "value": "test"}


def test_turso_cell_parsing():
    assert _parse_turso_cell({"type": "null", "value": None}) is None
    assert _parse_turso_cell({"type": "integer", "value": "100"}) == 100
    assert _parse_turso_cell({"type": "float", "value": 45.67}) == 45.67
    assert _parse_turso_cell({"type": "text", "value": "hello"}) == "hello"


def test_turso_client_init_url():
    client = TursoClient("libsql://my-db.turso.io", "test-token")
    assert client.http_endpoint == "https://my-db.turso.io/v2/pipeline"
    assert client.auth_token == "test-token"
    client.close()

    # Bearer token prefix stripping & pipeline suffix cleanup
    client2 = TursoClient("https://my-db.turso.io/v2/pipeline/", "Bearer secret_token_123")
    assert client2.http_endpoint == "https://my-db.turso.io/v2/pipeline"
    assert client2.auth_token == "secret_token_123"
    client2.close()


def test_turso_arg_special_values():
    assert _to_turso_arg(float("nan")) == {"type": "null"}
    assert _to_turso_arg(float("inf")) == {"type": "null"}
    assert _to_turso_arg(b"binary data") == {"type": "blob", "base64": "YmluYXJ5IGRhdGE="}
