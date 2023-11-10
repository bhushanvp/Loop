from datetime import datetime
from flask import Flask, request, jsonify
from confluent_kafka import Producer
import json
import os

app = Flask("My App")
REPORT_PATH = "./results/report.csv"
kafka_config = {
    "bootstrap.servers":'localhost:9092'
}
kafka_producer = Producer(kafka_config)


@app.route('/trigger_report', methods=['GET'])
def trigger_report():
    now = datetime.now()
    formatted_datetime = now.strftime("%Y%m%d_%H%M%S")
    reportname = f"{formatted_datetime}_report.csv"
    message = {
        "generate_report": reportname
    }
    kafka_producer.produce(topic="report_generation_topic",
                           value=json.dumps(message).encode(),
                           callback=producer_callback,
                           key=reportname)
    return jsonify({"message": reportname})


@app.route('/get_report', methods=['GET'])
def get_report():
    if os.path.exists(REPORT_PATH):
        # The report file is present; retrieve and return it
        report = retrieve_report_locally()
        os.remove()
        return jsonify({"status": "Complete", "report": report})
    else:
        # The report file is not present, indicating that CSV generation is still in progress
        return jsonify({"status": "Running"})


def producer_callback(self, err, msg):
    if err:
        print(err)
    else:
        print(msg)


def retrieve_report_locally():
    # Implement the logic to retrieve the report from local storage
    with open(REPORT_PATH, 'r') as file:
        report = file.read()
        return report


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
