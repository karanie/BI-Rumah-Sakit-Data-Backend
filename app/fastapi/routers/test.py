from typing import Annotated
import fastapi
from datastore.rdbms import DatastoreDB

router = fastapi.APIRouter()

@router.get("/api/fastapi/test")
async def test():
    return "hello from test router"
