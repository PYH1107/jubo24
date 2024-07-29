from fastapi import Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer
from auth0.utils import verify_jwt, get_user_roles, get_management_api_token, get_user_metadata
from auth0.models import TokenData
import logging

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_token_data(token: str = Depends(oauth2_scheme)) -> TokenData:
    payload = verify_jwt(token)
    roles = payload.get("https://myapp.example.com/roles", [])
    
    if isinstance(roles, str):
        roles = [roles]
    
    access_token = get_management_api_token()
    user_id = payload.get("sub")
    user_roles = get_user_roles(user_id, access_token)
    roles.extend([role['name'] for role in user_roles])
    
    app_metadata = get_user_metadata(user_id, access_token)

    return TokenData(
        sub=payload.get("sub"),
        permissions=payload.get("permissions", []),
        roles=roles,
        app_metadata=app_metadata
    )

def check_role(required_role: str):
    def role_checker(token_data: TokenData = Depends(get_token_data)):
        if required_role not in token_data.roles:
            raise HTTPException(status_code=403, detail="Not enough permissions")
        return token_data
    return role_checker

def check_permission(required_permission: str):
    def _check_permission(token_data: TokenData = Depends(get_token_data)):
        if required_permission not in token_data.permissions:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return token_data
    return _check_permission


 
#機構    
def check_organization_permission(token_data: TokenData, patient_organization_str: str):
    
    if not token_data or not token_data.app_metadata:
        raise HTTPException(status_code=403, detail="你根本沒有 metadata")

    user_organization = token_data.app_metadata.get('organization')
    if not user_organization or user_organization != patient_organization_str:
        raise HTTPException(status_code=403, detail="走開: 機構錯誤")
     
    logging.info(f"給過 機構存取～: {user_organization}")

#病患家屬
def check_patient_id_permission(token_data: TokenData, patient_id_str: str):
    if not token_data or not token_data.app_metadata:
        raise HTTPException(status_code=403, detail="你根本沒有 metadata")

    user_patient_id = token_data.app_metadata.get('patient_id')
    if not user_patient_id or user_patient_id != patient_id_str:
        raise HTTPException(status_code=403, detail="走開: 並非家屬")

    logging.info(f"給過 家屬存取～: {user_patient_id}")