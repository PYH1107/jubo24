import os
from flask import Flask, request, render_template, jsonify
from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common.flask_middleware import FlaskMiddleware
from linkinpark.lib.common.logger import getLogger, monitor
from time import perf_counter


ENVIRONMENT = os.getenv('ENV', 'test')
APP_NAME = 'FocusKnowledgeGraph'

lg = getLogger(APP_NAME, labels={
    'env': ENVIRONMENT, 
    'app': APP_NAME,
    'module': 'Server',
})

app = Flask(__name__)
app.logger = lg

app_with_middleware = FlaskMiddleware(app)


@app.route('/')
def index(methods=['GET']):
    return render_template('index.html', dropdown_focus=focus_list)


@app.route('/visual')
@monitor(logger_name=APP_NAME)
def get_graph(methods=['GET', 'POST']):
    current_focus = request.args.get('input_focus')

    node_dict = prepare_node_dict(current_focus)
    node_list = [{"data": node_dict[node]} for node in node_dict]
    edge_list = prepare_edge_list(current_focus, node_dict)

    return jsonify(elements={"nodes": node_list, "edges": edge_list})


def main():
    global bq_connector, mapping_table, focus_list, dirname
    bq_connector = BigQueryConnector()
    dirname = os.path.dirname(__file__)
    
    lg.info("Initializing: prepare focus mapping.")

    ts = perf_counter()

    time_start, time_end = get_time_range()

    mapping_table = prepare_focus_mapping(time_start, time_end)
    focus_list = prepare_focus_list(mapping_table)

    mapping_table['source'] = mapping_table['source_type'] + '_' + mapping_table['source_name']
    mapping_table['target'] = mapping_table['target_type'] + '_' + mapping_table['target_name']

    time_used = perf_counter() - ts
    
    lg.info(f'Service initialized in {time_used}.', metrics={'time_used': time_used})

    app.run(host='0.0.0.0', port=8080, debug=True)


def get_time_range():
    with open(os.path.join(dirname, 'get_latest_time_range.sql'), 'r') as fs:
        sql = fs.read()
    time_table, _ = bq_connector.execute_sql_in_bq(sql)
    time_range = time_table.to_records(index=False)[0]

    lg.info(f'Got latest mapping time range: {time_range}')

    return time_range


def prepare_focus_mapping(time_start, time_end):
    with open(os.path.join(dirname, 'get_mapping.sql'), 'r') as fs:
        sql = fs.read()
    mapping_table, _ = bq_connector.execute_sql_in_bq(
        sql.format(time_start=time_start, time_end=time_end))
    
    lg.info(f"Got the mapping table of the targets {mapping_table['target_type'].value_counts().to_dict()}")

    return mapping_table


def prepare_focus_list(mapping_table):
    focus_list = mapping_table[mapping_table['source_type'] == 'focus']['source_name'].unique()
    
    lg.info(f'Got {len(focus_list)} focuses', metrics={'focus_count': len(focus_list)})

    return focus_list


def prepare_node_dict(input_source_name):
    node_dict = {}
    id_count = 0
    
    for row_tuple in mapping_table[mapping_table['source_name'] == input_source_name].itertuples():

        if row_tuple.source not in node_dict:
            node_dict[row_tuple.source] = {"id": str(id_count), "name": row_tuple.source, "label": row_tuple.source_type}
            id_count += 1

        if row_tuple.target not in node_dict:
            node_dict[row_tuple.target] = {"id": str(id_count), "name": row_tuple.target, "label": row_tuple.target_type}
            id_count += 1
    
    return node_dict


def prepare_edge_list(input_source_name, node_dict):
    edge_list = []

    for row_tuple in mapping_table[mapping_table['source_name'] == input_source_name].itertuples():
        edge_list.append({
            "data": {
                "source": node_dict[row_tuple.source]['id'], 
                "target": node_dict[row_tuple.target]['id'],
                "relationship": str(row_tuple.count)
            }})
    
    return edge_list


if __name__ == '__main__':
    main()