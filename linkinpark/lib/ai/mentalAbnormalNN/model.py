from datetime import datetime
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from linkinpark.lib.ai.mentalAbnormalNN.utils import gen_features
from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common import logger


def get_model(cat, conn=BigQueryConnector()):
    col = {"act": "act_category", "bev": "bev_status"}
    df, _ = conn.execute_sql_in_bq(
        f"SELECT * "
        f"FROM jubo-ai.meta_prod_aiProduct.mentalAbnormality_{cat}Features"
    )
    model = df.set_index(col[cat]).dropna(how="any").to_dict(orient="index")
    model["date"] = datetime.now()
    return model


def update_model():
    global ACT_MODEL, BEV_MODEL
    ACT_MODEL, BEV_MODEL = get_model("act"), get_model("bev")
    logger.info(msg={
        "message": f"Model was updated on {datetime.utcnow()}."})


def compare_value(target, source):
    description = []
    avg_p = "{0:.2f}".format(
        abs(target["score_avg"] - source["score_avg"])
        / source["score_avg"] * 100)
    if target["score_avg"] < source["score_ll"]:
        description.append(f"期間平均精神分數低於過往紀錄{avg_p}%")
    elif target["score_avg"] > source["score_ul"]:
        description.append(f"期間平均精神分數高於過往紀錄{avg_p}%")
    sleep_t_p = "{0:.2f}".format(
        abs(target["asleep_avg"] - source["asleep_avg"])
        / source["asleep_avg"] * 100)
    if target["asleep_avg"] < source["asleep_ll"]:
        description.append(f"期間睡眠/補眠時間比低於過往紀錄{sleep_t_p}%")
    elif target["asleep_avg"] > source["asleep_ul"]:
        description.append(f"期間睡眠/補眠時間比高於過往紀錄{sleep_t_p}%")
    awake_t_p = "{0:.2f}".format(
        abs(target["awake_avg"] - source["awake_avg"])
        / source["awake_avg"] * 100)
    if target["awake_avg"] < source["awake_ll"]:
        description.append(f"期間清醒時間比低於過往紀錄{awake_t_p}%")
    elif target["awake_avg"] > source["awake_ul"]:
        description.append(f"期間清醒時間比高於過往紀錄{awake_t_p}%")
    active_t_p = "{0:.2f}".format(
        abs(target["active_avg"] - source["active_avg"])
        / source["active_avg"] * 100)
    if target["active_avg"] < source["active_ll"]:
        description.append(f"期間活躍時間比低於過往紀錄{active_t_p}%")
    elif target["active_avg"] > source["active_ul"]:
        description.append(f"期間活躍時間比高於過往紀錄{active_t_p}%")
    if target["wake_up_avg"] < source["wake_up_ll"]:
        description.append(f"期間醒來次數低於平常")
    elif target["wake_up_avg"] > source["wake_up_ul"] + 1:
        description.append(f"期間醒來次數高於平常")
    if target["fall_asleep_avg"] < source["fall_asleep_ll"]:
        description.append(f"期間睡著次數低於平常")
    elif target["fall_asleep_avg"] > source["fall_asleep_ul"] + 1:
        description.append(f"期間睡著次數高於平常")
    result = {"result": "正常" if len(description) == 0 else "異常",
              "description": description}
    return result


def act_abnormal_detection(act_content):
    act_content = act_content.dict()
    source = ACT_MODEL[act_content["activity"]]
    target = gen_features(act_content["start"], act_content["end"])
    result = compare_value(target, source)
    return result


def beh_abnormal_detection(beh_content):
    beh_content = beh_content.dict()
    source = BEV_MODEL[beh_content["status"]]
    target = gen_features(beh_content["start"], beh_content["end"])
    result = compare_value(target, source)
    return result


def render_result(content, result, model_type):
    origin_content = content.dict().copy()
    origin_content.update(result)
    result = jsonable_encoder(origin_content)
    logger.info(
        msg={
            "message": f"The model {model_type} has made a new prediction.",
            "content": result,
            "metrics": {"predict_count": 1},
        }
    )
    return JSONResponse(result, status_code=200)


ACT_MODEL = get_model("act")
BEV_MODEL = get_model("bev")
