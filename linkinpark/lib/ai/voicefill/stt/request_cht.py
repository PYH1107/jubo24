"""
Copied with minimal edit from https://gitlab.smart-aging.tech/ds/ai/ai-VoiceFill/-/blob/DEV/request_STT.py
"""

import requests
import json
import numpy as np
import wave

from linkinpark.lib.common.secret_accessor import SecretAccessor


class chuSTT:
    def __init__(self):
        sa = SecretAccessor()
        self.config = {
            "url_gettoken": "https://voicefilter-stg.ai.hinet.net/sdk/cloud/oauth/v2/Auth",
            "url_auth": 'https://voicefilter-stg.ai.hinet.net/sdk/cloud/stt/v1/Auth',
            "url_asr": "https://voicefilter-stg.ai.hinet.net/sdk/cloud/stt/v1/Asr",
            "domain": "jubo"

        }

        self.session = requests.Session()
        self.access_token = self.auth()
        self.result_partial = []  # 辨識中繼結果 整句
        self.words_partial = []  # 辨識中繼結果 word
        self.result = []  # 辨識結果 整句
        self.words = []  # 辨識結果 word

    def auth(self):
        auth_header = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }
        auth_params = {
            "Acc": "chttl004",
            "Pwd": "chttl004"
        }

        res = self.session.post(
            self.config['url_gettoken'], headers=auth_header, data=json.dumps(auth_params))
        res = json.loads(res.text)
        return res['access_token']

    def connect(self):

        header = {
            "access_token": self.access_token,
            "Content-type": "application/json",
            "accept": "application/json"
            # "x-client-id": "PythonTester",  # 測試者名稱,
            # "endpointmode":  "DoEndp"
        }
        data = {
            "domain": self.config['domain']
        }

        res = self.session.post(
            self.config["url_auth"], headers=header, json=data)
        res_json = json.loads(res.text)
        return res_json["AsrReferenceId"]

    def syncdata(self, Asref_id, voice_data):
        # 每次傳送長度，需進行調教，來達到較佳的辨識速度，建議值為0.08秒~0.15秒之間
        bytessend = 4800
        lens = len(voice_data)

        if len(voice_data) > bytessend:
            j = 0
            # 邊錄邊傳，此範例將音檔以每4800 byte(0.15秒)進行傳送，以模擬邊錄邊傳，透過以下API可以將音訊即時傳至後端辨識
            while j < lens:
                if j + bytessend > lens:
                    bytessend = lens - j
                header = {
                    "accept": "application/json",
                    "access_token": self.access_token
                }

                post_params = {
                    "AsrReferenceId": Asref_id,
                    "audioType": "pcm"
                }
                files = {
                    "data": voice_data[j:j + bytessend]
                }

                # 步驟四:開始傳送音訊buffer，取得辨識狀態'
                # time.sleep(0.15)
                res = self.session.post(
                    self.config["url_asr"], headers=header, data=post_params, files=files)
                # 步驟五:告知語音結束或已有辨識結果時，取得辨識結果
                # check AE-568 ticket to see sample cht return result
                res_json = json.loads(res.text)

                if "Result" in res_json and len(res_json["Result"]) > 0:
                    partial = False
                    # determine partial or full result
                    for seg in res_json["Result"]:
                        if "ResultType" in seg and seg["ResultType"] == "partial-result":
                            partial = True
                        # append results from api returns
                        if "Cans" in seg and len(seg["Cans"]) > 0:
                            for cans_seg in seg["Cans"]:
                                if "KeyPhr" in cans_seg:
                                    if partial:
                                        self.result_partial.append(
                                            cans_seg["KeyPhr"])
                                    else:
                                        self.result.append(cans_seg["KeyPhr"])
                                if "Words" in cans_seg:
                                    if partial:
                                        self.words_partial.append(
                                            cans_seg["Words"])
                                    else:
                                        self.words.append(cans_seg["Words"])

                j = j + bytessend

        return res_json

    def get_result(self, voice_data):
        AsrReferenceId = self.connect()
        res_json = self.syncdata(AsrReferenceId, voice_data)

        return res_json, (''.join(self.result).replace(" ", ""))

    def disconnect(self, Asref_id):
        params = {
            "Action": "stopRcg",
            "AsrReferenceId": Asref_id,
        }
        self.session.post(
            self.config["url"], headers=self.header, params=params, data="")

    @staticmethod
    def pcm_channels(wave_file):
        stream = wave.open(wave_file, "rb")

        num_channels = stream.getnchannels()
        sample_rate = stream.getframerate()
        sample_width = stream.getsampwidth()
        num_frames = stream.getnframes()

        raw_data = stream.readframes(num_frames)
        stream.close()

        audio = np.frombuffer(raw_data, dtype=np.int16)
        pcm_data = audio.astype(np.int16).tobytes()

        return pcm_data
