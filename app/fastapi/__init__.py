import fastapi

app = fastapi.FastAPI()

from .routers import test
from .routers import demografi
from .routers import pasien
from .routers import kunjungan
from .routers import utility
app.include_router(test.router)
app.include_router(demografi.router)
app.include_router(pasien.router)
app.include_router(kunjungan.router)
app.include_router(utility.router)
