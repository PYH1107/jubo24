import os
import json
from collections import Counter
import random
from flask import Flask, request, render_template, jsonify
import numpy as np
import logging
from argparse import ArgumentParser
from sklearn.neighbors import DistanceMetric
import requests

from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common.flask_middleware import FlaskMiddleware


def prepare_disease_patient_data():
    sql = '''SELECT *
             FROM `jubo-ai.app_prod_knowledgegraph.patientKG_triples`
             WHERE relation = "medicalHistory"'''
    disease_patients_df, _ = parser.client_bq.execute_sql_in_bq(sql)

    disease_patient_dict = {}
    for i in range(disease_patients_df.shape[0]):
        disease = disease_patients_df.loc[i, 'entity']
        if disease not in disease_patient_dict:
            disease_patient_dict[disease] = list()
        disease_patient_dict[disease].append(
            disease_patients_df.loc[i, 'patient'])

    disease_list = [str(i) for i in disease_patient_dict.keys()]

    dataset_id = 'raw_prod_datahub_mongo'
    sql = '''SELECT A._id AS org_id, A.nickName as org_name, SQ._id AS patient_id, SQ.lastName AS lastName, SQ.firstName AS firstName
             FROM `jubo-ai.%s.organizations` AS A
             INNER JOIN
                (SELECT A._id, A.organization, A.lastName, A.firstName FROM `jubo-ai.raw_prod_datahub_mongo.patients` AS A) AS SQ
             ON (A._id = SQ.organization)''' % dataset_id
    org_patients_df, _ = parser.client_bq.execute_sql_in_bq(sql)

    patient_name_dict = {}
    for i in range(org_patients_df.shape[0]):
        patient_name_dict[org_patients_df.loc[i, 'patient_id']
                          ] = org_patients_df.loc[i, 'lastName'] + org_patients_df.loc[i, 'firstName']

    return disease_patient_dict, patient_name_dict, disease_list


def query_patient_triples(patients, dataset_id):
    patient_list_sql = ' OR '.join(['(patient = "%s")' % i for i in patients])
    sql = '''SELECT patient, relation, entity
             FROM `jubo-ai.%s.patientKG_triples`
             WHERE %s''' % (dataset_id, patient_list_sql)
    triples, _ = parser.client_bq.execute_sql_in_bq(sql)
    return triples


def get_graph_node_edge():
    graph_node_list = []
    node_idx = 0

    node_dict = {}
    for patient in set(parser.triples["patient"]):
        graph_node_list.append({"data": {"id": '%s' % node_idx, "name": '%s' %
                               parser.patient_name_dict[patient], "label": '%s' % 'patient'}})
        node_dict[patient] = node_idx
        node_idx += 1

    entity_count_dict = dict(Counter(parser.triples["entity"]))
    conf_dict = {}
    for entity in entity_count_dict:
        conf = entity_count_dict[entity]
        if conf >= 3:
            conf_dict[entity] = 'strong'
        elif 1 < conf < 3:
            conf_dict[entity] = 'medium'
        elif conf == 1:
            conf_dict[entity] = 'weak'

    for entity in set(parser.triples["entity"]):
        conf = conf_dict[entity]
        graph_node_list.append(
            {"data": {"id": '%s' % node_idx, "name": '%s' % entity, "label": '%s_%s' % (conf, 'entity')}})
        node_dict[entity] = node_idx
        node_idx += 1

    graph_edge_list = []
    for i in range(parser.triples.shape[0]):
        patient = parser.triples.loc[i, "patient"]
        entity = parser.triples.loc[i, "entity"]
        source_id = node_dict[patient]
        target_id = node_dict[entity]
        relation = parser.triples.loc[i, "relation"]
        graph_edge_list.append({"data": {
                               "source": '%s' % source_id, "target": '%s' % target_id, "relationship": '%s' % relation}})
    return graph_node_list, graph_edge_list


def prepare_patient_df():
    dataset_id = 'app_prod_knowledgegraph'

    sql = '''select * from `%s.%s`''' % (dataset_id, 'patientKG_X')
    logging.info("start collecting patientKG_X")
    patient_df, _ = parser.client_bq.execute_sql_in_bq(sql)
    logging.info("%s patients data was collected" % patient_df.shape[0])
    patient_df.fillna(-1, inplace=True)
    patient_df.iloc[:, 1:] = patient_df.iloc[:, 1:].astype('float32')
    patient_df.set_index("patient", inplace=True)

    return patient_df


def get_significant_features(patient_df, the_relation_entity, smoothing=0.25):
    pos_df = patient_df[patient_df[the_relation_entity] == 1]
    pos_df = pos_df.drop([the_relation_entity], axis=1)
    neg_df = patient_df[patient_df[the_relation_entity] == 0]
    neg_df = neg_df.drop([the_relation_entity], axis=1)

    feature_dict = {}
    for col in list(pos_df.columns):
        std = np.std(patient_df[patient_df[col] != -1][col])
        pos_mean = pos_df[pos_df[col] != -1][col].mean()
        neg_mean = neg_df[neg_df[col] != -1][col].mean()
        diff = pos_mean - neg_mean
        if diff > smoothing * std:
            feature_dict[col] = str(round(diff, 1))
    return feature_dict


def get_significant_features_on_patient(patient_df, patient, smoothing=0.1):
    pos_df = patient_df[patient_df.index == patient]
    neg_df = patient_df[patient_df.index != patient]

    feature_dict = {}
    for col in list(pos_df.columns):
        if '_' in col:  # analyse numerical cols only
            continue
        std = np.std(patient_df[patient_df[col] != -1][col])
        pos_mean = pos_df[pos_df[col] != -1][col].mean()
        neg_mean = neg_df[neg_df[col] != -1][col].mean()
        diff = pos_mean - neg_mean
        if diff > smoothing * std:
            feature_dict[col] = str(round(diff, 1))
    return feature_dict


def get_focus_action_df():
    find_focus_action_df_sql = """SELECT *
                                  FROM `jubo-ai.aids_recommendation.focus_action_df`"""
    focus_action_df, _ = parser.client_bq.execute_sql_in_bq(
        find_focus_action_df_sql)

    focus_set = set(focus_action_df['focus'])
    return focus_action_df, focus_set


def query_action(patients, tmp_focus_action_df, n, focus):
    focus_kg_api = "https://focus-knowledge-graph-ge6dae6qzq-de.a.run.app/query"
    input_dict = {"input_focus": focus, "n": n}
    recommended = requests.post(focus_kg_api, json=input_dict).json()
    recommended_action = recommended["recommended_action"]

    result = []
    count = 0
    for p in patients:
        content = tmp_focus_action_df.loc[tmp_focus_action_df['patient'] ==
                                          p, 'content']
        if len(content) > 0:
            content = content.item().strip()
            for action in recommended_action:
                if (action in content) and (action not in result):
                    result.append(action)
                    count += 1
                    if count == n:
                        return result
    return result


app = Flask(__name__)
app_with_middleware = FlaskMiddleware(app)


@app.route("/query", methods=["POST"])
def query():
    data = request.get_json()
    relation = data['relation']
    entity = data['entity']

    the_relation_entity = '%s_%s' % (relation, entity)

    feature_dict = get_significant_features(
        parser.patient_df, the_relation_entity)
    return feature_dict


@app.route("/analyse", methods=["POST"])
def analyse():
    data = request.get_json()
    patient = data['patient']
    feature_dict = get_significant_features_on_patient(
        parser.patient_df, patient)
    return feature_dict


@app.route("/percentile_rank", methods=["POST"])
def percentile_rank(k=1000):
    data = request.get_json()
    patient = data['patient']

    similiar_patients = [patient] + \
        [p for p in neighbors(k=k)]
    patient_df = parser.patient_df.loc[similiar_patients]

    if "columns" in data:
        columns = data["columns"]
        patient_df = patient_df[columns]

    feature_pr_dict = {}
    for col in patient_df.columns:
        if '_' not in col:  # only numerical data included
            df = patient_df[patient_df[col] != -1]
            if patient in df.index:
                pr_result = df[col].rank(pct=True)
                patient_col_pr = pr_result[pr_result.index == patient].item()
                feature_pr_dict[col] = patient_col_pr
    return feature_pr_dict


@app.route("/neighbors", methods=["POST"])
def neighbors(compared_patient_df=None, k=10):
    data = request.get_json()
    patient = data['patient']
    if 'k' in data:
        k = data["k"]

    patient_df = parser.patient_df[parser.patient_df.index == patient]
    patient_available_columns = []
    for col in patient_df.columns:  # find similarity on patient available cols
        if patient_df[col].item() >= 0:
            patient_available_columns.append(col)
    patient_df = patient_df[patient_available_columns]

    if compared_patient_df is not None:
        tmp_patient_df = compared_patient_df[compared_patient_df.index !=
                                             patient][patient_available_columns]
    else:
        tmp_patient_df = parser.patient_df[parser.patient_df.index !=
                                           patient][patient_available_columns]

    if "columns" in data:
        columns = data["columns"]
        columns_list = []
        for col in columns:
            if col in patient_available_columns:
                columns_list.append(col)
            else:
                raise KeyError('no %s in patient KG' % col)
        patient_df = patient_df[columns_list]
        tmp_patient_df = tmp_patient_df[columns_list]

    numerical_cols = []
    categorical_cols = []
    for col in patient_df.columns:
        if '_' in col:
            categorical_cols.append(col)
        else:
            numerical_cols.append(col)

    distances_numerical = np.zeros(len(tmp_patient_df))
    if len(numerical_cols) > 0:
        patient_feature_numerical = patient_df[numerical_cols].to_numpy()
        others_feature_numerical = tmp_patient_df[numerical_cols].to_numpy()
        distances_numerical = parser.numerical_dist.pairwise(
            patient_feature_numerical.reshape(1, -1), others_feature_numerical).reshape(-1)

    distances_categorical = np.zeros(len(tmp_patient_df))
    if len(categorical_cols) > 0:
        patient_feature_categorical = patient_df[categorical_cols].to_numpy()
        others_feature_categorical = tmp_patient_df[categorical_cols].to_numpy(
        )
        distances_categorical = parser.categorical_dist.pairwise(
            patient_feature_categorical.reshape(1, -1), others_feature_categorical).reshape(-1)

    distances = distances_numerical + distances_categorical
    sorted = np.argsort(distances).reshape(-1)

    result = {}
    for idx in sorted[:k]:
        neighbor = tmp_patient_df.index[idx]
        result[neighbor] = str(distances[idx])
    return result


@app.route('/action', methods=["POST"])
def recommend_action(n=5, k=100):
    data = request.get_json()
    patient = data['patient']
    focus = data['focus']

    if focus not in parser.focus_set:
        return "No focus matched", 404

    tmp_focus_action_df = parser.focus_action_df[parser.focus_action_df['focus'] == focus]
    having_focus_patients = tmp_focus_action_df['patient']
    tmp_focus_patient_df = parser.patient_df.loc[having_focus_patients]

    if 'n' in data:
        n = data['n']
        assert n <= k

    the_focus_patients = list(
        neighbors(compared_patient_df=tmp_focus_patient_df, k=k))
    if patient in having_focus_patients:
        the_focus_patients = [patient] + the_focus_patients
        the_focus_patients = the_focus_patients[:n]

    action_list = query_action(
        the_focus_patients, tmp_focus_action_df, n, focus)
    return {'action': action_list}, 200


@app.route('/visual')
def get_graph(methods=['GET', 'POST']):
    current_disease = request.args.get('input_disease')
    patients = parser.disease_patient_dict[current_disease]
    patients = random.sample(patients, min(
        len(patients), parser.num_of_patients))

    parser.triples = query_patient_triples(
        patients, dataset_id='app_prod_knowledgegraph')

    graph_node_list, graph_edge_list = get_graph_node_edge()
    return jsonify(elements={"nodes": graph_node_list, "edges": graph_edge_list})


@app.route('/')
def index(methods=['GET']):
    return render_template('index.html', dropdown_diseases=parser.disease_list)


@app.route("/reload")
def reload():
    with open(__file__, "r") as f:
        content = f.readlines()
    with open(__file__, "w") as f:
        for line in content:
            f.write(line)
    return {}


def main():
    global parser
    parser = ArgumentParser(description='')

    parser.client_bq = BigQueryConnector()

    parser.patient_df = prepare_patient_df()

    disease_patient_dict, patient_name_dict, disease_list = prepare_disease_patient_data()
    parser.disease_patient_dict = disease_patient_dict
    parser.patient_name_dict = patient_name_dict
    parser.disease_list = disease_list

    parser.num_of_patients = 5

    parser.numerical_dist = DistanceMetric.get_metric('manhattan')
    parser.categorical_dist = DistanceMetric.get_metric('jaccard')

    parser.focus_action_df, parser.focus_set = get_focus_action_df()

    app.run(host='0.0.0.0', port=8080, debug=True)


if __name__ == '__main__':
    main()
