import os
import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from linkinpark.lib.common.postgres_connector import PostgresConnectorFactory
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from typing import List


# Set the URL prefix
ENTRY = "/ai-llm-agent-feedback"

# Directly set the database name
DB_NAME = "Oswin-test"

# FastAPI app initialization
app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.get("/ai-llm-agent-feedback/health-check")
async def health_check():
    app_version = os.environ.get("APP_VERSION")
    return {"status": "ok", "app_version": app_version}


# Request and Response Models
class Feedback(BaseModel):
    organization: str
    patient: str
    date: str
    generation_id: List[str]  # List of strings
    type: str  # good or bad
    review: str  # anything you want to say


class ResponseModel(BaseModel):
    message: str
    state: str


def get_db_connector():
    try:
        return PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname=DB_NAME,
        )
    except psycopg2.OperationalError as e:
        raise HTTPException(
            status_code=500, detail=f"Database connection failed: {str(e)}"
        )


@app.post(f"{ENTRY}/shiftrecord_feedback", response_model=ResponseModel)
async def submit_feedback(info: Feedback, connector=Depends(get_db_connector)):

    if not info.type and not info.review:
        raise HTTPException(
            status_code=400, detail="Either 'type' or 'review'.")

    try:
        query = '''
            INSERT INTO feedback
            (organization, patient, date, generation_id, type, review)
            VALUES (%s, %s, %s, %s, %s, %s)
        '''
        review_value = info.review if info.review is not None else ""

        for gen_id in info.generation_id:
            values = (info.organization,
                      info.patient,
                      info.date,
                      gen_id,
                      info.type,
                      review_value)
            connector.run_sql_execute(query.strip(), values)

        response_message = (
            f"Feedback received for generation_ids: "
            f"{', '.join(map(str, info.generation_id))}"
        )

        return ResponseModel(message=response_message, state="success")
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to submit feedback: {str(e)}"
        )

if __name__ == "__main__":
    uvicorn.run("linkinpark.app.ai.feedback.server:app",
                host="0.0.0.0", port=5000, proxy_headers=True)
