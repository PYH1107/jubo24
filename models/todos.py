from pydantic import BaseModel

class todo(BaseModel):
    name: str
    description: str
    complete: bool