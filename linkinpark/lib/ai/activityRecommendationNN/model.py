from datetime import datetime, timezone
from time import perf_counter

from bson.objectid import ObjectId
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sklearn.ensemble import RandomForestClassifier

from linkinpark.lib.ai.activityRecommendationNN.utils import LOG, gen_features, \
    get_top_three, prepare_data, get_activity_settings, get_category

EXPIRATION = 7


def train_model():
    x, y, schema = prepare_data()
    model = RandomForestClassifier(
        max_depth=100, random_state=0, class_weight="balanced")
    model.fit(x, y)
    result = {
        "date": datetime.now(tz=timezone.utc),
        "model": model,
        "schema": schema
    }
    return result


def check_model_expired(day):
    day_diff = datetime.now(tz=timezone.utc) - day
    expired = True if day_diff.days > EXPIRATION else False
    return expired


def update_model():
    LOG.info(f"Start updating the model.")
    train_st = perf_counter()
    global BLOCKED, MODEL
    if BLOCKED is False:
        BLOCKED = True
        MODEL = train_model()
        BLOCKED = False
        train_ed = perf_counter()
        sec = round(train_ed - train_st, 4)
        LOG.info(msg={
            "message": f"Model retrained, took {sec}'s.",
            "metrics": {"train_time": sec}})
    else:
        LOG.info(f"Model was blocked, unable to update the model.")
    return MODEL


def predict_result(content, model):
    day, classifier, schema = model.values()
    expired = check_model_expired(day)
    if expired:
        LOG.info(f"Model trained on {day} was expires on {datetime.utcnow()}.")
        model = update_model()
        day, classifier, schema = model.values()
    features = gen_features(schema, content)
    predictions = classifier.predict_proba(features)[0]
    top_three_prediction = get_top_three(predictions, schema)
    return top_three_prediction


def render_result(content):
    recommend_activity = predict_result(content, MODEL)
    result = content.dict().copy()
    result = {
        **result,
        "predicts": get_category(recommend_activity, SCHEMA),
        "details": recommend_activity,
        "version": MODEL["date"],
        "_id": str(ObjectId())
    }
    result = jsonable_encoder(result)
    LOG.info(msg={
        "message": "The model has made a new prediction.",
        "content": result,
        "metrics": {"predict_count": 1}})
    return JSONResponse(result, status_code=200)


def log_result(log_info):
    info = log_info.dict()
    LOG.info(msg={
        "message": f"For prediction {info['_id']}, the item "
                   f"{info['selected']} was selected.",
        "metrics": {"prediction_used": 1}})
    return JSONResponse(info, status_code=200)


MODEL = train_model()
SCHEMA = get_activity_settings()
BLOCKED = False
