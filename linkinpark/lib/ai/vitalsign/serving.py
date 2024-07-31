import json
from tqdm import tqdm

import linkinpark.lib.ai.vitalsign.utils as utils
from linkinpark.lib.common.gcs_helper import GcsHelper
from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common.date_processor import get_iso_day_string

GCS_BUCKET = 'vitalsign-model-storage'

default_vs_threshold = {
    "TP": {
        "minValue": "35.5",
        "maxValue": "38",
        "minValueSign": "<",
        "maxValueSign": ">"
    },
    "PR": {
        "minValue": "50",
        "maxValue": "120",
        "minValueSign": "<",
        "maxValueSign": ">"
    },
    "RR": {
        "minValue": "8",
        "maxValue": "25",
        "minValueSign": "<",
        "maxValueSign": ">"
    },
    "SYS": {
        "minValue": "80",
        "maxValue": "160",
        "minValueSign": "<",
        "maxValueSign": ">"
    },
    "DIA": {
        "minValue": "40",
        "maxValue": "100",
        "minValueSign": "<",
        "maxValueSign": ">"
    },
    "SPO2": {
        "minValue": "90",
        "maxValue": "100",
        "minValueSign": "<",
        "maxValueSign": ">"
    },
    "PAIN": {
        "minValue": "0",
        "maxValue": "10",
        "minValueSign": "<",
        "maxValueSign": ">"
    }
}


class ServingVitalsign:
    def __init__(self):
        self.gcs = GcsHelper()
        self.bq_connector = BigQueryConnector()
        self.model = None
        self.scaler = None
        self.explainer = None

        mapping_table_patient = self.gcs.download_by_string_from_bucket("vitalsign-model-storage",
                                                                        "mapping_table/patient_table.pkl")
        self.mapping_table_patient_dict = {
            (row['original_organization_id'] + row['original_patient_id']): row['patient_id'] for index, row in
            mapping_table_patient.iterrows()}

        mapping_table_organization = self.gcs.download_by_string_from_bucket("vitalsign-model-storage",
                                                                             "mapping_table/organization_table.pkl")
        self.mapping_table_organization_dict = {
            (row['original_organization_id']): row['organization_id'] for index, row in
            mapping_table_organization.iterrows()}

        try:
            self.vsTreshold = self.gcs.download_by_string_from_bucket('vitalsign-model-storage',
                                                                      f"threshold_setting/{get_iso_day_string(1)}.pkl")
            self.vsTreshold['threshold'] = self.vsTreshold['threshold'].apply(
                lambda x: json.loads(x))
        except AttributeError:
            self.vsTreshold = default_vs_threshold

    def _initial_org_model(self):
        self.org_model = {
            "model": {},
            "scaler": {},
            "explainer": {}
        }

    def load_model_scaler(self, path):
        self.model = self.gcs.download_by_string_from_bucket(
            GCS_BUCKET, 'model/' + path + '_model.pkl')
        self.scaler = self.gcs.download_by_string_from_bucket(
            GCS_BUCKET, 'scaler/' + path + '_scaler.pkl')

    def load_explainer(self, path):
        self.explainer = self.gcs.download_by_string_from_bucket(GCS_BUCKET,
                                                                 'explainer/' + path + '_explainer.pkl')

    def load_org_model_into_memory(self):
        self._initial_org_model()
        model_blobs = [blob for blob in self.gcs.get_blobs_list(
            GCS_BUCKET, "model/") if "org" in blob]
        scaler_blobs = [blob for blob in self.gcs.get_blobs_list(
            GCS_BUCKET, "scaler/") if "org" in blob]
        explainer_blobs = [blob for blob in self.gcs.get_blobs_list(
            GCS_BUCKET, "explainer/") if "org" in blob]
        progess = tqdm(total=len(model_blobs))

        for index in range(len(model_blobs)):

            self.org_model["model"][model_blobs[index]] = self.gcs.download_by_string_from_bucket(GCS_BUCKET,
                                                                                                  model_blobs[index])
            self.org_model["scaler"][scaler_blobs[index]] = self.gcs.download_by_string_from_bucket(GCS_BUCKET,
                                                                                                    scaler_blobs[index])
            try:
                self.org_model["explainer"][explainer_blobs[index]] = self.gcs.download_by_string_from_bucket(
                    GCS_BUCKET,
                    explainer_blobs[
                        index])
            except IndexError:
                pass

            progess.update(1)

    def load_org_model(self, orgId, _type):
        self.model = self.org_model['model']["model/org_" +
                                             orgId + "_" + _type + "_model.pkl"]
        self.scaler = self.org_model['scaler']["scaler/org_" +
                                               orgId + "_" + _type + "_scaler.pkl"]

    def load_org_explainer(self, orgId, _type):
        self.explainer = self.org_model['explainer']["explainer/org_" +
                                                     orgId + "_" + _type + "_explainer.pkl"]

    def predict_by_ai(self, X, existedCol, importantTreshold=-1):
        X = self.scaler.transform(X)
        if 'SYS' in existedCol:
            index = existedCol.index('SYS')
            X[:, index] = utils.expRight(X[:, index])

        y_score = self.model.decision_function(X)[0]
        importances = []
        if y_score < 0:
            if self.explainer:
                shap_values = self.explainer(X).values[0]
                self.explainer = None
                for i in range(len(existedCol)):
                    if shap_values[i] < importantTreshold:
                        importances.append(existedCol[i])
            else:
                importances.append(existedCol[0])

        return [y_score, importances]

    def predict_by_rule(self, vsData, existedCol, vsTreshold):
        y_score = 1
        importances = []
        for itemName in existedCol:
            # check if PAIN=None or PAIN=null
            if itemName == 'PAIN' and not str(vsData[itemName]).isnumeric():
                value = 0
            else:
                value = float(vsData[itemName])
            minValueRule = value < float(vsTreshold[itemName]['minValue'])
            maxValueRule = value >= float(vsTreshold[itemName]['maxValue'])
            if itemName == 'SPO2':
                isAbnormal = minValueRule and value <= 100
            elif itemName == 'PAIN':
                isAbnormal = value > 0
            else:
                isAbnormal = (minValueRule or maxValueRule)
            if isAbnormal:
                y_score = -1
                importances.append(itemName)
        return [y_score, importances]
