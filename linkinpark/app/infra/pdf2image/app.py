import uvicorn
import os
import uuid
import subprocess
import threading
import requests
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from google.cloud import storage
from queue import Queue

from linkinpark.lib.common.logger import getLogger
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware


APP_NAME = os.getenv("APP_NAME", "infra-pdf2image")
APP_ENV = os.getenv("APP_ENV", "dev")
APP_RESOLUTION = os.getenv("APP_RESOLUTION", "500")


logger = getLogger(
    name=APP_NAME,
    labels={
        "app": APP_NAME,
        "env": APP_ENV
    })


app = FastAPI()
app.add_middleware(FastAPIMiddleware, path_prefix="/infra-pdf2image")

tasks = []
task_queue = Queue()


class Task(BaseModel):
    id: str
    gcs_path: str
    status: str


class TaskList(BaseModel):
    tasks: List[Task]


class SubmitTaskRequest(BaseModel):
    gcs_path: str
    pages: List[int]
    page_format: int


class TaskStatusResponse(BaseModel):
    status: str
    queue_position: int = None


def convert_pdf_pages(pdf_path, output_prefix, pages, page_format):
    output_files = []
    for page in pages:
        output_file = f"{output_prefix}"
        output_files.append(output_file + f"-{str(page).zfill(page_format)}.jpg")
        try:
            subprocess.run([
                "pdftoppm", "-jpeg", "-r", f"{APP_RESOLUTION}", "-f", str(
                    page), "-l", str(page),
                pdf_path, output_file.replace(".jpg", "")
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.info(f"Error occurred: {e}")
    return output_files


def process_pdf_task(task_id: str, gcs_path: str, pages: list, page_format: int):
    try:
        logger.info(f"Commencing processing of task with ID {task_id}.")
        client = storage.Client()
        logger.info(f"Commencing processing of task with gcs {gcs_path}.")
        bucket_name, pdf_name = gcs_path.replace("gs://", "").split("/", 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(pdf_name)
        pdf_file = pdf_name.split("/")[-1]
        output_blob = pdf_name.replace(pdf_file, "")
        local_pdf_path = f"/tmp/{pdf_file}"
        blob.download_to_filename(local_pdf_path)

        output_prefix = f"/tmp/{task_id}"
        output_files = convert_pdf_pages(local_pdf_path, output_prefix, pages, page_format)
        
        for output_file in output_files:
            out_path = f"{output_blob}image/{os.path.basename(output_file)}"
            blob = bucket.blob(out_path)
            logger.info(f"Complete convert to gcs {out_path}.")
            blob.upload_from_filename(output_file)

        logger.info(f"Task {task_id} completed successfully.")
        update_task_status(task_id, "completed")
        result_to_familyline(task_id, "completed")
    except Exception as e:
        logger.info(f"Task {task_id} failed with error: {e}")
        update_task_status(task_id, "failed")
        result_to_familyline(task_id, f"failed:{e}")


def update_task_status(task_id: str, status: str):
    for task in tasks:
        if task.id == task_id:
            task.status = status
            break


def task_consumer():
    while True:
        task_id, gcs_path, pages, page_format = task_queue.get()
        update_task_status(task_id, "in_progress")
        process_pdf_task(task_id, gcs_path, pages, page_format)
        task_queue.task_done()


def result_to_familyline(task_id, status):
    url = f'https://ai-model-{APP_ENV}.jubo.health/ds-family-line-bot/task_notify'
    payload = json.dumps({"task_id": task_id,
                          "status": status
                          })
    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=payload)
    logger.info(f"Task {task_id} status to familyline: {response.text}.")


@app.post("/infra-pdf2image/submit_task", response_model=str)
async def submit_task(request: SubmitTaskRequest):
    task_id = str(uuid.uuid4())
    logger.info(f"Task with ID {task_id} has been received")
    task = Task(id=task_id, gcs_path=request.gcs_path, status="queue")
    tasks.append(task)
    task_queue.put((task_id, request.gcs_path, request.pages, request.page_format))
    return task_id


@app.get("/infra-pdf2image/task_status/{task_id}", response_model=TaskStatusResponse)
async def task_status(task_id: str):
    last_completed_index = -1
    for i in range(len(tasks) - 1, -1, -1):
        if tasks[i].status == "completed":
            last_completed_index = i
            break

    for index, task in enumerate(tasks):
        if task.id == task_id:
            if task.status == "queue":
                if last_completed_index == -1:
                    queue_position = index + 1
                else:
                    queue_position = index - last_completed_index
                return TaskStatusResponse(status=task.status, queue_position=queue_position)
            return TaskStatusResponse(status=task.status)
    raise HTTPException(status_code=404, detail="Task not found")


@app.get("/infra-pdf2image/task_list", response_model=TaskList)
async def task_list():
    return TaskList(tasks=tasks)


def main():
    consumer_thread = threading.Thread(target=task_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    uvicorn.run("linkinpark.app.infra.pdf2image.app:app",
                host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == "__main__":
    main()
    
