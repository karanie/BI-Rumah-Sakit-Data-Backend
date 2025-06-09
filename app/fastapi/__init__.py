import hmac
import fastapi
import config

api_key_header = fastapi.security.APIKeyHeader(name="X-Api-Key", auto_error=False)
async def get_api_key(api_key: str = fastapi.Security(api_key_header)):
    if not config.API_KEY:
        return

    if not api_key:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized"
        )

    if not hmac.compare_digest(api_key, config.API_KEY):
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key."
        )

    return api_key

app = fastapi.FastAPI(dependencies=[fastapi.Depends(get_api_key)])

from .routers import test
from .routers import demografi
from .routers import pasien
from .routers import kunjungan
from .routers import pendapatan
from .routers import utility
app.include_router(test.router)
app.include_router(demografi.router)
app.include_router(pasien.router)
app.include_router(kunjungan.router)
app.include_router(pendapatan.router)
app.include_router(utility.router)
