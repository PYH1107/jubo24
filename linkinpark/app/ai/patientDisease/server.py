import pickle
import json
import os
import pandas as pd

import uvicorn
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common.fastapi_middleware import FastAPIMiddleware 
from linkinpark.lib.common.file_editor import FileEditor
from linkinpark.lib.common.gcs_helper import GcsHelper
# from linkinpark.lib.common.model_uploader import ModelUploader


global bq_client, columns_dict, disease_model
APP_NAME = 'test-app-patientdisease'
MODEL_NAME = 'patientdisease-lightgbm'

app = FastAPI(title='Patient Disease APP')
app.add_middleware(FastAPIMiddleware)


def download_model(model_name, app_name):
    bucket_name = 'jubo-ai-models'
    gcs_helper = GcsHelper()
    download_dir = os.getcwd()
    model_list = gcs_helper.get_blobs_list(bucket_name, f'{app_name}/{app_name}_{model_name}')
    latest_model = max(model_list)
    model_id = os.path.splitext(os.path.basename(latest_model))[0]

    app_name, _, __ = model_id.partition('_')
    model_path = gcs_helper.get_blobs_list(bucket_name, f'{app_name}/{model_id}')[0]
    
    download_path = os.path.join(download_dir, model_id)
    dest_name = os.path.join(download_dir, os.path.split(model_path)[-1])
    gcs_helper.download_file_from_bucket(bucket_name, model_path, dest_name)
    FileEditor.extract_zip(dest_name, download_path)
    os.remove(dest_name)
    return download_path


# model_uploader = ModelUploader()
# model_path = model_uploader.download_model(MODEL_NAME, APP_NAME)
model_path = download_model(MODEL_NAME, APP_NAME)


def load_model():
    disease_list = ['medicalHistory_HighBP', 'medicalHistory_Diabetes', 'medicalHistory_Dementia', 'medicalHistory_CVD',
                    'medicalHistory_COPD', 'medicalHistory_CVA', 'medicalHistory_Arthritis', 'medicalHistory_UTI',
                    'medicalHistory_HeartDisease', 'medicalHistory_BPH', 'medicalHistory_KidneyDisease', 'medicalHistory_PN',
                    'medicalHistory_Cancer', 'medicalHistory_Neurosis', 'medicalHistory_Parkinson']

    disease_model = {}
    for disease in disease_list:
        disease_name = disease.split('_')[1]
        with open(os.path.join(model_path, f'{disease_name}.pkl'), 'rb') as f:
            disease_model[disease] = pickle.load(f)
    return disease_model


def load_columns():
    with open(os.path.join(model_path, 'columns.json'), 'r') as f:
        columns_dict = json.load(f)
    return columns_dict


def missing_imputation(patient_data, columns_dict):
    columns = patient_data.columns[patient_data.isna().any()].tolist()
    for col in columns:
        if columns_dict[col] is not None:
            patient_data[col] = columns_dict[col]
        elif col.startswith('medicalHistory'):
            patient_data[col] = 0
    return patient_data


def normal_imputation(patient_data):
    normal_values = {
        'age': 30,          # Age in years
        'SYS': 110,         # Systolic Blood Pressure in mmHg
        'PR': 75,           # Pulse Rate in beats per minute
        'RR': 16,           # Respiratory Rate in breaths per minute
        'SPO2': 98,         # Oxygen Saturation in %
        'DIA': 70,          # Diastolic Blood Pressure in mmHg
        'AC': 120,          # Blood Sugar (after eating) in mg/dL
        'weight': 68.18,    # Weight in kg (150 pounds)
        'TP': 37.0,         # Temperature in degrees Celsius
        'height': 172.72,   # Height in cm (68 inches)
    }
    columns = patient_data.columns[patient_data.isna().any()].tolist()
    for col in columns:
        if col in normal_values:
            patient_data[col] = normal_values[col]
        else:
            patient_data[col] = 0
    patient_data['bmi'] = float(patient_data['weight']) / \
        (float(patient_data['height']) / 100)**2
    return patient_data


def predict(patient_data):
    disease_dict = {}
    for disease in disease_model:
        model = disease_model[disease]
        x = patient_data.drop(['%s' % disease], axis=1).to_numpy()
        prob = round(model.predict_proba(x)[0][1], 4)
        disease_dict[disease] = prob
    return disease_dict


bq_client = BigQueryConnector()
columns_dict = load_columns()
disease_model = load_model()


class PatientDiseaseInput(BaseModel):
    patient_id: str = Field(example='5d7f6b463ae64d002d8275ad')


class PatientDiseaseOutput(BaseModel):
    medicalHistory_HighBP: float
    medicalHistory_Diabetes: float
    medicalHistory_Dementia: float
    medicalHistory_CVD: float
    medicalHistory_COPD: float
    medicalHistory_CVA: float
    medicalHistory_Arthritis: float
    medicalHistory_UTI: float
    medicalHistory_HeartDisease: float
    medicalHistory_BPH: float
    medicalHistory_KidneyDisease: float
    medicalHistory_PN: float
    medicalHistory_Cancer: float
    medicalHistory_Neurosis: float
    medicalHistory_Parkinson: float


@app.get("/")
async def root():
    return RedirectResponse("/docs")


@app.post('/predict/route1', response_model=PatientDiseaseOutput)
async def predict_by_patient(patient_disease_input: PatientDiseaseInput):
    columns = ", ".join([i for i in columns_dict])
    sql = """SELECT %s FROM `jubo-ai.app_prod_knowledgegraph.patientKG_X` WHERE patient = '%s'""" % (
        columns, patient_disease_input.patient_id)
    patient_data, _ = bq_client.execute_sql_in_bq(sql)
    if len(patient_data) == 0:
        raise HTTPException(
            status_code=404, detail="Patient %s not existed" % patient_disease_input.patient_id)
    patient_data = missing_imputation(patient_data, columns_dict)

    disease_dict = predict(patient_data)
    return disease_dict


@app.post('/predict/route2', response_model=PatientDiseaseOutput)
async def predict_by_features(features_input: dict):
    cols_list = list(columns_dict.keys())
    patient_data = pd.DataFrame(features_input, columns=cols_list, index=[0])
    patient_data = normal_imputation(patient_data)
    disease_dict = predict(patient_data)
    return disease_dict


def main():
    uvicorn.run('linkinpark.app.ai.patientDisease.server:app', 
                host="0.0.0.0", port=5000, proxy_headers=True)


if __name__ == '__main__':
    main()
