from typing import Annotated
import fastapi
from datastore.rdbms import pl_read_database

router = fastapi.APIRouter()

@router.get("/api/fastapi/test")
async def test():
    return "hello from test router"
