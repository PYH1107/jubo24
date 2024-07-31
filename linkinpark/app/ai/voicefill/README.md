# VoiceFill - Extract vital signs from audio

This API will return vital signs parsed from an audio file. 

## Outline
- [Features](#features)
- [Usage](#usage)
- [Dependencies](#dependencies)

## Features
The algorithm contains two parts: 

* Transform audio to text (Speech-to-Text, STT)
* Parse vital signs from the transcription text

Currently, we use Chunghwa Telecom's API service to transform audio to text and regular expression to parse vital signs from transcripts.

## Usage
This API follows common FastAPI framework. See `test/app/ai/voicefill` for examples to invoke the endpoints.

### !!! Important !!!
Chunghwa Telecom's API has certain requirements for the input `.wav` file:

* It only supports pcm-16, i.e.,16kHz voice. 
* It only supports Mono (single channel), not Stereo. 


## Dependencies
- [FastAPI - framework, high performance, easy to learn, fast to code, ready 
for production](https://fastapi.tiangolo.com/)
- [Uvicorn - The lightning-fast ASGI server](https://www.uvicorn.org/)
- [python-multipart](https://pypi.org/project/python-multipart/)