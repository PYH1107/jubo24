
# Infra PDF to Image API Documentation

## Overview

This API provides functionality to convert PDF pages to images. It uses Google Cloud Storage (GCS) for storing the input PDF files and the output image files.

## Base URL

`https://ai-model-release.jubo.health/api/v1/infra-pdf2image`

## Endpoints

### 1. Submit Task

#### Endpoint

`POST /submit_task`

#### Description

Submits a new task to convert specified pages of a PDF file stored in GCS to images.

#### Request

- **Headers:**
  - `Content-Type: application/json`
  - `Authorization: Bearer <token>`

- **Body:**
  ```json
  {
      "gcs_path": "gs://<bucket_name>/<file_name>.pdf",
      "pages": [<page_numbers>]
  }
  ```

#### Response

- **Body:**
  ```json
  "<task_id>"
  ```

#### Curl Example

```sh
curl --location 'https://ai-model-release.jubo.health/api/v1/infra-pdf2image/submit_task' \
--header 'Content-Type: application/json' \
--header 'Authorization: Bearer <token>' \
--data '{
    "gcs_path": "gs://test_airbyte_temp/shit.pdf",
    "pages": [1, 2, 3]
}'
```

### 2. Get Task Status

#### Endpoint

`GET /task_status/{task_id}`

#### Description

Retrieves the status of a submitted task.

#### Request

- **Headers:**
  - `Authorization: Bearer <token>`

#### Response

- **Body:**
  ```json
  {
      "status": "<status>",
      "queue_position": <queue_position> // optional, present only if the status is "queue"
  }
  ```

#### Curl Example

```sh
curl --location 'https://ai-model-release.jubo.health/api/v1/infra-pdf2image/task_status/{task_id}' \
--header 'Authorization: Bearer <token>'
```

### 3. Get Task List

#### Endpoint

`GET /task_list`

#### Description

Retrieves a list of all submitted tasks along with their statuses.

#### Request

- **Headers:**
  - `Authorization: Bearer <token>`

#### Response

- **Body:**
  ```json
  {
      "tasks": [
          {
              "id": "<task_id>",
              "gcs_path": "gs://<bucket_name>/<file_name>.pdf",
              "status": "<status>"
          },
          ...
      ]
  }
  ```

#### Curl Example

```sh
curl --location 'https://ai-model-release.jubo.health/api/v1/infra-pdf2image/task_list' \
--header 'Authorization: Bearer <token>'
```

## Models

### Task

- **id** (string): Unique identifier for the task.
- **gcs_path** (string): GCS path of the PDF file.
- **status** (string): Status of the task (`queue`, `in_progress`, `completed`, `failed`).

### TaskList

- **tasks** (List[Task]): List of tasks.

### SubmitTaskRequest

- **gcs_path** (string): GCS path of the PDF file.
- **pages** (List[int]): List of page numbers to be converted to images.

### TaskStatusResponse

- **status** (string): Status of the task.
- **queue_position** (int, optional): Position in the queue if the status is `queue`.

## Notes

- Ensure the `Authorization` header contains a valid bearer token.
- Replace `<task_id>` with the actual task ID when querying task status.
- Replace `<bucket_name>` and `<file_name>` with the actual GCS bucket name and file name.



