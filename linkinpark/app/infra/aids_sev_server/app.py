"""
The server support on collecting webhook message from monitoring resource
    (ex. cloud monitoring, cloud error report, grafana, airflow .. etc)
    and send notification message to jira and google chat uniformly.
"""

import os
import json
import requests
import logging
from datetime import datetime, timedelta, timezone

from jira import JIRA
import flask
from flask import request


from linkinpark.lib.common.gcs_helper import GcsHelper
from linkinpark.lib.common.flask_monitor import FlaskMonitorServing
from linkinpark.app.infra.aids_sev_server.message_parser import parser_cloud_monitoring, parse_airflow_message


JIRA_USERID_MAP = {
    "dennis.liu": "61cd2b71e763790068b483ff",
    "@lucychou": "621d97159c3cce00694a664b",
    "Roy Lai": "62c2d84266d158d6546e2d37",
    "Chiang Shen Ju": "603db29bcc13b600698f1e75",
}
GCHAT_USERID_MAP = {
    "dennis.liu": "113348480736822596744",
    "@lucychou": "115156034355464726251",
    "Roy Lai": "111664796426229134055",
    "Chiang Shen Ju": "107756714165903791081",
    "kevin.xu": "107791232670019951409",
    "harvey.wu": "115578510873222788504",
}


JIRA_CONNECTION = JIRA(
    basic_auth=('dennisliu@jubo.health', 'qUuhExoeJ86MqbaCdhsxBBC3'),
    server="https://jubo-ai-ds.atlassian.net"
)

GCP_MONITOR_KEYS = set({'version', 'incident'})
DATARCH_KEYS = set({'type', 'env', 'dag', 'url'})

# Not support yet
GCP_ERROR_REPORT_KEYS = set({'version', 'subject', 'group_info',
                             'exception_info', 'event_info'})
GRAFANA_KEYS = set({'receiver', 'status', 'alerts', 'groupLabels', 'commonLabels', 'commonAnnotations',
                   'externalURL', 'version', 'groupKey', 'truncatedAlerts', 'orgId', 'title', 'state', 'message'})

# message backup gcs path
BUCKET_NAME = "jubo-ai-serving"
BLOB_PREFIX = "infra_aids_sev_server"

app = flask.Flask(__name__)
app_with_monitor = FlaskMonitorServing(app)
gcs_helper = GcsHelper()


def get_now_str():
    dt_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    dt_tw = dt_utc.astimezone(timezone(timedelta(hours=8)))

    return dt_tw.strftime("20%y-%m-%d %H:%M:%S")


def send_jira(res):
    """
    Create issue in jira from parsing result
    Args:
        res <dict>: 
            {
            "title": <jira card title>,
            "poc":  <owner of card>,
            "url": url,
            "type": <cloud monitoring | airflow>,
            "create_dt": datetime
            }
    """
    issue_dict = {
        'project': {'key': 'AISEV'},
        'summary': res['title'],
        'description': f"{res['title']} \n link: {res['url']}",
        'issuetype': {'name': 'Task'},
        "assignee": {"accountId": JIRA_USERID_MAP.get(res['poc'], "61cd2b71e763790068b483ff")},
    }
    issue = JIRA_CONNECTION.create_issue(fields=issue_dict)
    issue_url = f"https://jubo-ai-ds.atlassian.net/jira/core/projects/AISEV/board?selectedIssue={issue.key}"

    return issue_url


def send_chat(res, issue_url):
    """
    Send notification message to google chat with parsing result
    Args:
        res <dict>: 
            {
            "title": <jira card title>,
            "poc":  <owner of card>,
            "url": url,
            "type": <cloud monitoring | airflow>,
            "create_dt": datetime
            }
        issue_url <str>: jira url
    """
    url = 'https://chat.googleapis.com/v1/spaces/AAAAsxud6uk/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=zHSIzzS3Vvvg-04EDnUoDWdrfYxLp1xyhvoH6g-Fwvg%3D'
    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}
    text = {"text": "出事了阿伯 v2.0 ~~~ \n"}
    text['text'] += f"*Summary*: {res['title']} \n"
    text['text'] += f"*POC*: <users/{GCHAT_USERID_MAP.get(res['poc'], '113348480736822596744')}> \n"
    text['text'] += f"*Issue Link*:{issue_url} \n"
    text['text'] += f"*Error Link*:{res['url']} \n"
    response = requests.request(
        "POST", url, headers=message_headers, data=json.dumps(text))
    logging.info(response)


@app.route("/")
def home():
    return "Hello, Flask!"


@app.route('/webhook', methods=['POST'])
def webhook():
    now = get_now_str()
    if request.method == 'POST':
        message = request.get_json()
        res = None

        msg_keys = set(message.keys())
        logging.info(msg_keys)
        # GCP monitoring
        if len(msg_keys & GCP_MONITOR_KEYS) / len(GCP_MONITOR_KEYS) > 0.8:
            res = parser_cloud_monitoring(message, now)
            logging.info(res)
        # airflow
        if len(msg_keys & DATARCH_KEYS) / len(DATARCH_KEYS) > 0.8:
            res = parse_airflow_message(message, now)
            logging.info(res)

        if res:
            issue_url = send_jira(res)
            send_chat(res, issue_url)
            file_path = f"alerts/{res['type']}-{now}.json"
        else:
            file_path = f"alerts/{now}.json"

        with open(file_path, 'w') as f:
            json.dump(message, f)

        blob_path = os.path.join(BLOB_PREFIX, file_path)
        gcs_helper.upload_file_to_bucket(BUCKET_NAME, blob_path, file_path)

    return message


def main():
    os.makedirs('alerts/', exist_ok=True)
    os.makedirs('error-alerts/', exist_ok=True)
    app_with_monitor.run()


if __name__ == '__main__':
    main()
