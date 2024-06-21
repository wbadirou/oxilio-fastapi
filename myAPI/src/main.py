import uvicorn
from fastapi import FastAPI
from src.routers import castorAPI

app = FastAPI()


@app.get("/client/client_state", tags=["Clients"])
async def client_state(dbname):
    q_result = castorAPI.get_client_active(dbname)
    result = castorAPI.format_result(q_result)
    return result


@app.get("/welcome", tags=["resources"])
async def welcome_message():
    return {"msg": castorAPI.welcome()}


@app.get("/healthcheck", tags=["resources"])
async def healthcheck(checkCode=str):
    result = castorAPI.healthcheck(checkCode)
    return {"result": result}


'''@app.get("/db/dblist", tags=["DBTools"])
async def list_db():
    result = castorAPI.list_existing_db()
    return {"databases": result}'''


@app.get("/db/gen", tags=["DBTools"])
async def gen(dbname):
    q_result = castorAPI.get_gen_table(dbname)
    result = castorAPI.format_result(q_result)
    return {"result": result}


@app.get("/db/rta", tags=["DBTools"])
async def rta(dbname, limit=int(500)):
    q_result = castorAPI.get_rta_table(dbname, limit)
    result = castorAPI.format_result(q_result)
    return {"result": result}


if __name__ == '__main__':
    uvicorn.run("src.main:app", host="127.0.0.1", port=8001, reload=True)
