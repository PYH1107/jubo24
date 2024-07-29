import requests
from jose import jwt
from fastapi import HTTPException
from auth0.config import AUTH0_DOMAIN, ALGORITHMS, API_IDENTIFIER, CLIENT_ID, CLIENT_SECRET

# Get JWKS
def get_jwks():
    jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    response = requests.get(jwks_url)
    return response.json()

jwks = get_jwks()

# Get Public Key
def get_public_key(token):
    unverified_header = jwt.get_unverified_header(token)
    rsa_key = {}
    for key in jwks["keys"]:
        if key["kid"] == unverified_header["kid"]:
            rsa_key = {
                "kty": key["kty"],
                "kid": key["kid"],
                "use": key["use"],
                "n": key["n"],
                "e": key["e"]
            }
    return rsa_key

# Verify JWT
def verify_jwt(token):
    rsa_key = get_public_key(token)
    try:
        payload = jwt.decode(
            token,
            rsa_key,
            algorithms=ALGORITHMS,
            audience=API_IDENTIFIER,
            issuer=f"https://{AUTH0_DOMAIN}/"
        )
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.JWTClaimsError:
        raise HTTPException(status_code=401, detail="Invalid token claims")
    except Exception:
        raise HTTPException(status_code=401, detail="Unable to parse authentication token.")

def get_management_api_token():
    url = f"https://{AUTH0_DOMAIN}/oauth/token"
    headers = {
        'content-type': 'application/json'
    }
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'audience': f"https://{AUTH0_DOMAIN}/api/v2/",
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    token = response.json()['access_token']
    return token

def get_user_roles(user_id, access_token):
    url = f"https://{AUTH0_DOMAIN}/api/v2/users/{user_id}/roles"
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_user_metadata(user_id, access_token):
    url = f"https://{AUTH0_DOMAIN}/api/v2/users/{user_id}"
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    user_info = response.json()
    return user_info.get("app_metadata", {})