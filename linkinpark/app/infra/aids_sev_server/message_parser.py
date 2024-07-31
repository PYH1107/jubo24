import json
import requests
import logging


def parse_airflow_message(message, now):
    try:
        if message['env'] == 'prod':
            dag_name = message['dag']

            url = f"https://gitlab.smart-aging.tech/api/v4/projects/159/repository/commits?path=dags/{dag_name}.py"

            payload = json.dumps({
                "env": "datarch"
            })
            headers = {
                'PRIVATE-TOKEN': 'Kw6nDzAn9YsTpgzzNwYa',
                'Content-Type': 'application/json',
                'Cookie': '_gitlab_session=483be4351179ab931231ba69f6e2a041'
            }

            response = requests.request(
                "GET", url, headers=headers, data=payload)
            author_list = []
            for commit in response.json():
                author_list.append(commit['author_name'])
            author_name = max(author_list, key=author_list.count)
            url = message['url']
            title = f"[datarch] {message['dag']} task failed !"

            return {
                "title": title,
                "poc": author_name,
                "url": url,
                "type": "pipeline",
                "create_dt": now
            }
    except Exception as e:
        logging.error(e)
        return None


def parser_cloud_monitoring(message, now):
    try:
        url = message['incident']['url']
        title = f"[cloud monitoring] {message['incident']['summary']}"

        poc = 'dennis.liu'
        msg_type = "gcp_services"
        return {
            "title": title,
            "poc": poc,
            "url": url,
            "type": msg_type,
            "create_dt": now
        }
    except Exception as e:
        logging.error(e)
        return None
