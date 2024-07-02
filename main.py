from typing import Annotated

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from passlib.hash import bcrypt

from FastAPI.myAPI.myAPI.src.routers import castorAPI

from pydantic import BaseModel

SECRET_KEY = "iytv2ZZcjuSN1Dw2Y4vF5bsfBmmON0vDf/WNwIwcRVQ="
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: str or None = None


class User(BaseModel):
    username: str
    email: str or None = None
    full_name: str or None = None
    disabled: bool or None = None
    hashed_password: str


class UserInDB(User):
    hashed_password: str


pwd_context = CryptContext(schemes=["sha256_crypt", "md5_crypt", "des_crypt"], default="des_crypt")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def get_user(username: str):
    q_result = castorAPI.get_api_users()
    result = castorAPI.format_result(q_result)
    for user in result:
        if username == user["username"]:
            user_data = User(username=user["username"], email=user["email"], full_name=user["full_name"],
                             disabled=user["disabled"], hashed_password=user["hashed_password"])
            return user_data


def authenticate_user(username: str, password: str):
    user = get_user(username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False

    return user


def create_access_token(data: dict, expires_delta: timedelta or None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now() + expires_delta
    else:
        expire = datetime.now() + timedelta(minutes=45)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credential_exception = HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                                         detail="Could not validate credentials",
                                         headers={"WWW-Authenticate": "Bearer"})
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credential_exception

        token_data = TokenData(username=username)
    except JWTError:
        raise credential_exception

    user = get_user(username=token_data.username)
    if user is None:
        raise credential_exception

    return user


async def get_current_active_user(current_user: UserInDB = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")

    return current_user


@app.post("/token", response_model=Token, tags=["Authentification"])
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Incorrect username or password", headers={"WWW-Authenticate": "Bearer"})
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/users/me/", response_model=User, tags=["Authentification"])
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user


@app.get("/users/me/items", tags=["Authentification"])
async def read_own_items(current_user: User = Depends(get_current_active_user)):
    return [{"item_id": 1, "owner": current_user}]


@app.get("/client/client_state", tags=["Clients"])
async def client_state(dbname, username, password):
    q_result = castorAPI.get_client_active(dbname, username, password)
    result = castorAPI.format_result(q_result)
    return result


@app.get("/welcome", tags=["resources"])
async def welcome_message():
    return {"msg": castorAPI.welcome()}


@app.get("/healthcheck", tags=["resources"])
async def healthcheck(checkCode=str):
    result = castorAPI.healthcheck(checkCode)
    return {"result": result}


@app.get("/db/gen/{username}", tags=["DBTools"])
async def gen(dbname, username, password):
    q_result = castorAPI.get_gen_table(dbname, username, password)
    result = castorAPI.format_result(q_result)
    return {"result": result}


@app.get("/db/gen_decrypted", tags=["DBTools"])
async def gen_decrypted(dbname, username, password):
    q_result = castorAPI.get_gen_table_decrypted(dbname, username, password)
    result = castorAPI.format_result(q_result)
    return {"result": result}


@app.get("/oxilioclients", tags=["Client Tools"])
async def oxilioclients():
    q_result = castorAPI.get_oxilio_clients()
    result = castorAPI.format_result(q_result)
    return {"result": result}


@app.get("/db/rta", tags=["DBTools"])
async def rta(dbname, username, password, limit=int(500)):
    q_result = castorAPI.get_rta_table(dbname, username, password, limit)
    result = castorAPI.format_result(q_result)
    return {"result": result}


@app.post("/createclient/{client_id}/", tags=["Client tools"])
async def createclient(isActive, username, password, client_id, client_name):
    q_result = castorAPI.create_client_db(isActive, client_id, client_name, username, password)
    return q_result

@app.post("/register", tags=["Client tools"])
async def register(username, pwd, full_name, email, disabled):
    hashed_password = get_password_hash(pwd)
    print(hashed_password)
    q_result = castorAPI.create_api_user(username, hashed_password, full_name, email, disabled)
    return q_result


@app.put("/deactivateclient", tags=["Client tools"])
async def deactivate_client(dbname, username, password, client_id):
    q_result = castorAPI.deactivate_client(dbname, username, password, client_id)
    return q_result


@app.post("/updateclient/{clientid}/", tags=["Client tools"])
async def update_client(dbname, username, password, isactive, clientid, clientname, systemapi, key1, secret1, key2,
                        secret2, apiid, hist):
    q_result = castorAPI.update_client(dbname, username, password, isactive, clientid, clientname, systemapi, key1,
                                       secret1, key2, secret2, apiid, hist)
    return q_result
