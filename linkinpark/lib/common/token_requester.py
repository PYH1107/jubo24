import requests

from linkinpark.lib.common.secret_accessor import SecretAccessor


class TokenRequester:
    def __init__(self):
        self.sa = SecretAccessor()
        self.body = {}
        self.headers = {}

    def get_secrets(self, secrets: dict) -> None:
        """
        Get secrets from google secret manager and update request body accordingly

        Args:
            secrets         dict containing key of request body and secret name to call from. E.g., 
                            {"username": "aiServerAuth-ds-manage-assistant-line-bot-email", 
                             "password": "aiServerAuth-ds-manage-assistant-line-bot-password"}

        Return:
            None
        """
        for sk, sv in secrets.items():
            self.body[sk] = self.sa.access_secret(sv)

    def update_headers(self, headers: dict) -> None:
        """
        Update request headers. 
        
        Args: 
            headers         headers for request. E.g., {"Content-Type": "application/json"}
            
        Return:
            None 
        """
        self.headers.update(headers)

    def request_token(self, url: str, **kwargs) -> dict:
        response = requests.post(url, json=self.body, headers=self.headers, **kwargs)
        return response.json()
    
