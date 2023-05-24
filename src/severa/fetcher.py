import os

from httpx import AsyncClient

SEVERA_CLIENT_ID = os.getenv("SEVERA_CLIENT_ID")
SEVERA_CLIENT_SECRET = os.getenv("SEVERA_CLIENT_SECRET")
SEVERA_SCOPE = (
    "customers:read,settings:read,invoices:read,"
    "projects:read,users:read,resourceallocations:read,"
    "hours:read"
)
SEVERA_BASE_URL = "https://api.severa.visma.com/rest-api/v1.0"


async def authenticate(client: AsyncClient):
    payload = {
        "client_Id": SEVERA_CLIENT_ID,
        "client_Secret": SEVERA_CLIENT_SECRET,
        "scope": SEVERA_SCOPE,
    }

    response = await client.post("token", json=payload)
    response.raise_for_status()

    auth = response.json()

    return {
        "client_Id": SEVERA_CLIENT_ID,
        "Authorization": f"{auth['access_token_type']} " f"{auth['access_token']}",
    }


async def fetch():
    async with AsyncClient(base_url=SEVERA_BASE_URL, timeout=60.0) as client:
        auth_header = await authenticate(client)

        return await client.get("users", headers=auth_header).json()
