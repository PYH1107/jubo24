import logging
from flask import Flask, request, jsonify
from linkinpark.lib.ai.bloodsugar.model import AnomalyDetector
from linkinpark.lib.common.flask_monitor import FlaskMonitorServing
from linkinpark.lib.common.flask_middleware import FlaskMiddleware


app = Flask(__name__)
app_with_monitor = FlaskMonitorServing(app)
app_with_middleware = FlaskMiddleware(app)
detector = AnomalyDetector()


@app.route('/predict', methods=['POST'])
def predict():
    data = request.get_json(force=True)
    logging.info(f'Got {len(data)} data.')
    try:
        return jsonify({'results': [detector.detect(x) for x in data], 'state': 'success'}), 200
    except Exception as e:
        logging.warning(e)
        return jsonify({'message': e, 'state': 'success'}), 200


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s - %(levelname)s] %(message)s',
        datefmt='%d-%b-%y %H:%M:%S'
    )
    app.run(port=5000, host='0.0.0.0')


if __name__ == "__main__":
    main()
