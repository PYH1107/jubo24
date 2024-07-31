import numpy as np
import pandas as pd
from datetime import timedelta, datetime


def getTimestamp():
    timestamp = '[' + str(datetime.today() + timedelta(hours=8)) + ']'
    return timestamp


def getLabel(row):
    try:
        return 0 if pd.isna(row.trackStatus) else 1
    except Exception as e:
        print(e)
        return 0


def filterNoise(row, vsItemsByModel):
    for name in vsItemsByModel:
        if name not in row:
            continue
        if row[name] == 0 or row[name] >= 300:
            return False
    return True


def intersecList(a, b):
    return list(set(a).intersection(set(b)))


def getMlColsList(keylist, vsItemsGroup):
    mlColList = []
    for group in vsItemsGroup:
        existedCol = intersecList(keylist, group['usedCol'])
        if len(existedCol) > 0:
            mlColList.append({
                'existedCol': sorted(existedCol),
                'contRatio': group['contRatio'],
                'vote': group['vote'],
            })
    return mlColList


def getContamination(data, ratio):
    labelSeries = data['label']
    con = max(labelSeries.mean(), 0.1)
    return con * ratio


def getCleanData(data, hourPassThreshold=-8):
    return data[(data['label'] == 0) & (data['hourPass'] < hourPassThreshold)]


def expRight(array):
    return np.exp(array * 2) - 1
