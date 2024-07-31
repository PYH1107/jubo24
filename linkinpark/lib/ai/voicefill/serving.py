import json
import requests

from .stt.request_cht import chuSTT
from .extract.re import value_extraction 


class ServingVoicefill:
    def __init__(self, host):
        self.host = host

    def send_wav_to_text(self, wav_filename):
        with open(wav_filename, "rb") as file:
            DataBuffer = file.read()
        AcSTT = chuSTT()
        voicetext = AcSTT.get_result(DataBuffer)

        return voicetext

    def post_text_file(self, text):
        word_data = {"word": text}
        response = requests.post(self.host, json=word_data)  # Use the json parameter instead of data

        return response.json()

    def transcribe(self, wav_filename):
        voice2text_result = self.send_wav_to_text(wav_filename)
        va = ValueExtractor()
        vital_result = va.extract_value(voice_textresult)

        return {
            'voice2text_result': voice2text_result,
            'vital_result': vital_result
        }