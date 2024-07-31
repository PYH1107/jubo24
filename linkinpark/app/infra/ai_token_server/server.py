import os
import logging
from typing import Dict
from functools import wraps
from datetime import datetime, timedelta
from fastapi.middleware.cors import CORSMiddleware


import jwt
import hashlib
import uvicorn
import requests
import urllib.parse
from fastapi import FastAPI, Request

from linkinpark.lib.common.secret_accessor import SecretAccessor

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

TOKEN_CACHE: Dict[str, str] = {}
BAD_TOKEN_CACHE: Dict[str, int] = {}

ACCESSOR = None


@app.get("/token/health-check")
async def health_check():
    app_version = os.environ.get("APP_VERSION")
    return {"status": "ok", "app_version": app_version}


def _init_accessor(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        global ACCESSOR
        if not ACCESSOR:
            ACCESSOR = SecretAccessor()
        return func(*args, **kwargs)
    return wrapper


def get_token(username: str, password: str) -> str:
    # Generate the hash value of the username and password as the cache key
    cache_key = generate_cache_key(username, password)
    if cache_key in BAD_TOKEN_CACHE:
        if BAD_TOKEN_CACHE[cache_key] > 3:
            raise ValueError(
                "Maximum limit for username and password inputs reached.")

    # Check if there is a valid token in the cache
    if cache_key in TOKEN_CACHE:
        cached_token = TOKEN_CACHE[cache_key]
        # Check if the token is expired
        if not is_token_expired(cached_token):
            return cached_token

    # Generate a new token
    new_token = generate_token(username, password)

    if new_token:
        # Store the new token in the cache
        TOKEN_CACHE[cache_key] = new_token
        return new_token
    else:
        if cache_key in BAD_TOKEN_CACHE:
            BAD_TOKEN_CACHE[cache_key] += 1
        else:
            BAD_TOKEN_CACHE[cache_key] = 1

        raise ValueError("Invalid username or password")


def is_token_expired(token: str) -> bool:
    # Implement the logic to check if the token is expired
    try:
        creation_time = get_token_creation_time(token)
        expiration_time = creation_time + timedelta(hours=1)
        return datetime.now() > expiration_time
    except ValueError:
        return True


def authenticate_user(username: str, password: str) -> bool:
    # Implement user authentication logic here
    # Assume it always returns True, indicating that any username and password are verified
    return True


@_init_accessor
def generate_token(username: str, password: str) -> str:
    # Implement the logic to generate a JWT token
    # Here, we simply use the username as the token
    auth_url = ACCESSOR.access_secret("tokenServer-auth0-url")
    client_secret = ACCESSOR.access_secret("tokenServer-auth0-client_secret")
    client_id = ACCESSOR.access_secret("tokenServer-auth0-client_id")
    audience = ACCESSOR.access_secret("tokenServer-auth0-audience")

    username_encode = urllib.parse.quote_plus(username)
    audience_encode = urllib.parse.quote_plus(audience)

    payload = f'grant_type=password&username={username_encode}&password={password}&audience={audience_encode}&client_id={client_id}&client_secret={client_secret}&scope=openid'
    headers = {
        'content-type': 'application/x-www-form-urlencoded',
    }

    try:
        response = requests.request(
            "POST", auth_url, headers=headers, data=payload)
        access_token = response.json()['access_token']
        return access_token
    except Exception as e:
        logging.error(f"auth0 login error:{e}")
        return False


def get_token_creation_time(token: str) -> datetime:
    try:
        decoded_token = jwt.decode(
            token, options={"verify_signature": False}, algorithms=["HS256"])
        if "iat" in decoded_token:
            timestamp = decoded_token["iat"]
            return datetime.fromtimestamp(timestamp)
        else:
            raise ValueError("Invalid token format: missing 'iat' claim")
    except jwt.exceptions.InvalidTokenError as e:
        raise ValueError("Invalid token: " + str(e))


def generate_cache_key(username: str, password: str) -> str:
    # Generate the hash value of the username and password as the cache key
    hash_value = hashlib.sha256((username + password).encode()).hexdigest()
    return hash_value


@app.post("/token")
async def get_token_endpoint(request: Request):
    try:
        data = await request.json()
        username = data.get("username")
        password = data.get("password")
        token = get_token(username, password)
        return {"token": token}
    except ValueError as e:
        return {"error": str(e)}


def main():
    uvicorn.run("linkinpark.app.infra.ai_token_server.server:app",
                host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == "__main__":
    main()
