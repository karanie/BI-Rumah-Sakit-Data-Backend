import fastapi

app = fastapi.FastAPI()

from .routers import test
from .routers import demografi
app.include_router(test.router)
app.include_router(demografi.router)
