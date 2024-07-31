import numpy as np
import pandas as pd
import requests

import linkinpark.lib.ai.vitalsign.utils as utils
from linkinpark.lib.ai.vitalsign.logger import logger
from linkinpark.lib.ai.vitalsign.serving import ServingVitalsign, default_vs_threshold
from linkinpark.lib.common.secret_accessor import SecretAccessor

vs = ServingVitalsign()
sa = SecretAccessor()

vsItemsByModel = ['TP', 'PR', 'SYS', 'DIA', 'SPO2']
vsItemsByRule = ['PAIN', 'RR']
vsItemsGroup = [
    {
        'usedCol': ['SYS', 'DIA', 'PR'],
        'contRatio': 1,
        'vote': {
            'ps_if': 0.4,
            'all_if': 0.6,
        },
    },
    {
        'usedCol': ['TP', 'SPO2'],
        'contRatio': 0.5,
        'vote': {
            'ps_if': 0.4,
            'all_if': 0.3,
            'rule': 0.3,
        },
    },
]


def vsModel(vsData):
    patientIdExist = True
    orgIdExist = True
    vsTreshold = vs.vsTreshold

    # filter empty values
    vsData = {key: value for key, value in vsData.items() if value != ''}

    try:
        patientId = vs.mapping_table_patient_dict[vsData['organization'] +
                                                  vsData['patient']]
    except Exception as e:
        logger.debug("patientIdExist=False")
        logger.debug(e)
        patientIdExist = False

    try:
        orgId = vs.mapping_table_organization_dict[vsData['organization']]
    except Exception as e:
        logger.debug("orgIdExist=False")
        logger.debug(e)
        orgIdExist = False

    if 'sendBackUrl' in vsData:
        sendBackUrl = vsData['sendBackUrl']
    else:
        sendBackUrl = ''

    try:
        vsTreshold = vsTreshold.loc[vsTreshold['organization_id'] ==
                                    orgId].iloc[0]['threshold']
            
    # if org not exist vsTreshold will be empty
    # we need to given default threshold
    except Exception as e:
        logger.debug("Use default vs threshold")
        vsTreshold = pd.DataFrame.from_dict(default_vs_threshold)

    intersectedItems = utils.intersecList(vsData.keys(), vsItemsByModel)
    mlColslist = utils.getMlColsList(intersectedItems, vsItemsGroup)

    importantItems = []
    usedModel = []
    predictedValue = 1

    for mlDic in mlColslist:
        scores = {}
        existedCol = mlDic['existedCol']
        model_type = '_'.join(sorted(existedCol))
        X = np.array([[float(vsData[name]) for name in existedCol]])

        # Personalized Isolation Forest
        if patientIdExist:
            try:
                if len(existedCol) > 1:
                    vs.load_explainer('ps_' + patientId + '_' + model_type)
                vs.load_model_scaler('ps_' + patientId + '_' + model_type)
                scores['ps_if'] = vs.predict_by_ai(X, existedCol)
                scores['ps_if'][0] *= mlDic['vote']['ps_if']
                usedModel.append('ps_if')
            except Exception as e:
                logger.debug(e)

        # Organization Isolation Forest
        if orgIdExist:
            try:
                if len(existedCol) > 1:
                    vs.load_org_explainer(orgId, model_type)
                vs.load_org_model(orgId, model_type)
                scores['all_if'] = vs.predict_by_ai(X, existedCol)
                scores['all_if'][0] *= mlDic['vote']['all_if']
                usedModel.append('all_if')
            except Exception as e:
                logger.debug(e)

        # Rule
        if 'rule' in mlDic['vote'].keys() or len(scores.keys()) == 0:
            scores['rule'] = vs.predict_by_rule(vsData, existedCol, vsTreshold)
            scores['rule'][0] *= mlDic['vote'].get('rule') or 1
            usedModel.append('rule')

        # Vote
        sumScores = 0
        sumImportances = []
        for key in scores.keys():
            sumScores += scores[key][0]
            sumImportances += scores[key][1]
        if sumScores < 0:
            predictedValue = -1
            if 'rule' in scores.keys():
                importantItems += scores['rule'][1]
            else:
                importantItems += list(set(sumImportances))

    # check PAIN and RR
    intersectedItems = utils.intersecList(vsData.keys(), vsItemsByRule)
    [y_score, importances] = vs.predict_by_rule(
        vsData, intersectedItems, vsTreshold)
    if y_score < 0:
        predictedValue = -1
        importantItems += importances

    # Store to db
    predictResult = {'label': predictedValue,
                     'importantItems': importantItems,
                     'rule': ','.join(set(usedModel)),
                     'vitalsign': vsData.get('_id', ''),
                     'organization': vsData.get('organization', ''),
                     'patient': vsData['patient'],
                     'createdDate': vsData.get('createdDate', ''),
                     '__v': 0}

    if sendBackUrl != '':
        try:
            requests.post(sendBackUrl, headers={"APIKEY": sa.access_secret("nis-server-access-apikey")},
                          json=predictResult)
            logger.debug(' Result sent! Vitalsign: ' +
                         vsData['_id'])
        except Exception as e:
            logger.error(
                f' Result NOT sent! Vitalsign: {vsData["_id"]}. Error: {e}')

    return predictResult
