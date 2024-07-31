from datetime import datetime
import pandas as pd
import json
import requests
from linkinpark.lib.common.gcs_helper import GcsHelper


GCS_BUCKET = "jubo-ai-serving"
GCS_BLOB_PREFIX = "/ai-bloodsugar/"


class Utils(GcsHelper):
    def __init__(self, neighbor_service_url):
        super().__init__()
        self.full_bloodsugars_df = None
        self.patients_df = None
        self.last_update_time = datetime.min
        self.neighbor_service_url = neighbor_service_url

    def load_df_from_gcs(self, table_name):
        return self.download_by_string_from_bucket(GCS_BUCKET, f"{GCS_BLOB_PREFIX}{table_name}.pkl")

    def trim_sort_df(self, bloodsugars_df, sugar_type):
        bloodsugars_df = bloodsugars_df[bloodsugars_df.sugarType == sugar_type]
        bloodsugars_df = bloodsugars_df.sort_values(
            "createdDate", ascending=False).head(20)
        return bloodsugars_df["sugarValue"]

    def get_history_data(self, patient, sugar_type):
        if (datetime.now() - self.last_update_time).days >= 1:
            # save the dataframes so that we do not need to download them multiple times
            # update the dataframe only if it has been more than a day since last update
            self.full_bloodsugars_df = self.load_df_from_gcs("bloodsugars")
            self.last_update_time = datetime.now()
        bloodsugars_df = self.full_bloodsugars_df[self.full_bloodsugars_df.patient == patient]
        return self.trim_sort_df(bloodsugars_df, sugar_type)

    def get_similar_patients_data(self, patient, sugar_type):
        neighbors = json.loads(requests.post(
            url=self.neighbor_service_url,
            json={"patient": patient, "k": 50}
        ).text)
        neighbors = sorted(neighbors, key=neighbors.get, reverse=True)
        bloodsugars_df = self.full_bloodsugars_df[self.full_bloodsugars_df.patient.isin(
            neighbors)]
        return self.trim_sort_df(bloodsugars_df, sugar_type)

    def check_diabetes(self, patient):
        if isinstance(self.patients_df, pd.DataFrame):
            self.patients_df = self.load_df_from_gcs("patients")
        self.patients_df = self.patients_df[self.patients_df._id == patient]
        medical_history = self.patients_df.iloc[0]["medicalHistory"].lower()
        keywords = ["diabetes", "hyperglycaemia", "糖尿", "血糖"]
        for keyword in keywords:
            if keyword in medical_history:
                return True
        return False
