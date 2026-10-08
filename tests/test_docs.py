import httpx

from app.core.config import get_settings
from app.main import app


async def test_documentation_is_public_and_payments_require_api_key():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        docs = await client.get("/docs")
        assert docs.status_code == 200
        assert "/openapi.json" in docs.text
        response = await client.get("/openapi.json")
        assert response.status_code == 200
        schema = response.json()
        security = schema["components"]["securitySchemes"]["ApiKeyAuth"]
        assert security["type"] == "apiKey"
        assert security["in"] == "header"
        assert security["name"] == "X-API-Key"
        assert get_settings().api_key.get_secret_value() not in response.text
        for path, method in [
            ("/api/v1/payments", "post"),
            ("/api/v1/payments/{payment_id}", "get"),
        ]:
            assert schema["paths"][path][method]["security"] == [{"ApiKeyAuth": []}]
        assert (await client.get("/health")).status_code == 401
        assert (await client.post("/api/v1/payments", json={})).status_code == 401


async def test_openapi_describes_idempotency_examples_and_errors():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        schema = (await client.get("/openapi.json")).json()
    create = schema["paths"]["/api/v1/payments"]["post"]
    header = next(item for item in create["parameters"] if item["name"] == "idempotency-key")
    assert header["required"] is True
    assert header["in"] == "header"
    assert {"202", "401", "409", "413", "422"} <= create["responses"].keys()
    assert "Location" in create["responses"]["202"]["headers"]
    assert schema["components"]["schemas"]["PaymentCreate"]["examples"][0]["amount"] == "100.50"
    assert "404" in schema["paths"]["/api/v1/payments/{payment_id}"]["get"]["responses"]

    amount = schema["components"]["schemas"]["PaymentDetail"]["properties"]["amount"]
    assert amount["type"] == "string"
    assert amount["example"] == "100.50"
