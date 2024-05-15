import fastapi
from .sensors.controller import router as sensorsRouter
from yoyo import read_migrations
from yoyo import get_backend

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")


backend = get_backend('postgresql://postgres:postgres@postgreSQL/postgres')
migrations = read_migrations('../migrations_ts')

with backend.lock():
    backend.apply_migrations(backend.to_apply(migrations))
    backend.rollback_migrations(backend.to_rollback(migrations))
    

app.include_router(sensorsRouter)

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}