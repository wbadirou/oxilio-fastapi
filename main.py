from typing import Optional

from fastapi import FastAPI

import castorAPI

from pydantic import BaseModel

app = FastAPI()


class Client(BaseModel):
    is_active: bool
    client_id: int
    client_name: str


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

@app.get("/db/gen_decrypted", tags=["DBTools"])
async def gen_decrypted(dbname):
    q_result = castorAPI.get_gen_table_decrypted(dbname)
    result = castorAPI.format_result(q_result)
    return {"result": result}

@app.get("/oxilioclients", tags=["Client Tools"])
async def oxilioclients():
    q_result = castorAPI.get_oxilio_clients()
    result = castorAPI.format_result(q_result)
    return {"result": result}


@app.get("/db/rta", tags=["DBTools"])
async def rta(dbname, limit=int(500)):
    q_result = castorAPI.get_rta_table(dbname, limit)
    result = castorAPI.format_result(q_result)
    return {"result": result}


@app.post("/createclient", tags=["Client tools"])
async def createclient(isActive, client_id, client_name):
    q_result = castorAPI.create_client_db(isActive, client_id, client_name)
    return q_result


@app.put("/deactivateclient", tags=["Client tools"])
async def deactivate_client(dbname, client_id):
    q_result = castorAPI.deactivate_client(dbname, client_id)
    return q_result

@app.post("/updateclient", tags=["Client tools"])
async def update_client(dbname, isactive, clientid, clientname, systemapi, key1, secret1, key2, secret2, apiid, hist):
    q_result = castorAPI.update_client(dbname, isactive, clientid, clientname, systemapi, key1, secret1, key2, secret2, apiid, hist)
    return q_result
