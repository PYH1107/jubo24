import flask
from linkinpark.lib.ai.riskAnalysis.serving import ServingRiskAnalysis
from linkinpark.lib.common.flask_monitor import FlaskMonitorServing
from linkinpark.lib.common.flask_middleware import FlaskMiddleware

app = flask.Flask(__name__)
app_with_monitor = FlaskMonitorServing(app)
app_with_middleware = FlaskMiddleware(app)
app.config['JSON_AS_ASCII'] = False

ra = ServingRiskAnalysis()


@app.route("/predict", methods=["POST"])
def predict():
    output_dict = {}
    if flask.request.method == "POST":
        try:
            data = flask.request.get_json(force=True)
        except Exception as e:
            print(e, flush=True)
            return flask.jsonify({'Error': e}), 200

        result = ra.predict(data)
        output_dict["result"] = result

    return flask.jsonify(output_dict), 200


def main():
    app.run(port=5000, host="0.0.0.0")


if __name__ == "__main__":
    main()
