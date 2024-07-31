# FaceVitalsign - Extract vital signs from video

This API will return vital signs parsed from an video file. 

## Outline
- [Features](#features)
- [Usage](#usage)
- [Dependencies](#dependencies)

## Features
Construct a shell for a third part API provider.

> Currently, we use Azure's API service to upload video and retrive vital signs as response.

## Usage
This API follows common FastAPI framework. See `test/app/ai/faceVitalsign` for examples to invoke the endpoints.


## Dependencies
- [FastAPI - framework, high performance, easy to learn, fast to code, ready 
for production](https://fastapi.tiangolo.com/)
- [Uvicorn - The lightning-fast ASGI server](https://www.uvicorn.org/)
- [python-multipart](https://pypi.org/project/python-multipart/)