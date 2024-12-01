import connexion
from connexion import NoContent
import json
import datetime
import os
import requests
import yaml
import logging
import logging.config
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

# Load configurations
env = os.environ.get("TARGET_ENV", "dev")
app_conf_file = "/config/app_conf.yml" if env == "test" else "app_conf.yml"
log_conf_file = "/config/log_conf.yml" if env == "test" else "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')
logger.info(f"Running in {env.upper()} environment")
logger.info(f"App Conf File: {app_conf_file}")
logger.info(f"Log Conf File: {log_conf_file}")

# Constants
HIGH_VALUE = app_config["thresholds"]["high_value"]
LOW_VALUE = app_config["thresholds"]["low_value"]
data_store = app_config['data_store']['filename']

logger.info(f"Anomaly thresholds - High Value: {HIGH_VALUE}, Low Value: {LOW_VALUE}")

# Ensure datastore exists
if not os.path.exists(data_store):
    logger.info(f"Creating data store: {data_store}")
    with open(data_store, 'w') as f:
        json.dump([], f)

def find_anomalies():
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group',
        reset_offset_on_start=False,
        auto_offset_reset=OffsetType.LATEST
    )

    with open(data_store, 'r') as f:
        anomalies = json.load(f)

    for msg in consumer:
        msg_str = msg.value.decode("utf-8")
        event = json.loads(msg_str)
        logger.info(f"Message received: {event}")

        payload = event["payload"]
        event_type = event["type"]
        price = payload["price"]
        current_timestamp = datetime.datetime.now().isoformat()

        anomaly = None

        if event_type == "buy" and price <= LOW_VALUE:
            anomaly = {
                "event_id": payload["user_id"],
                "trace_id": payload["trace_id"],
                "event_type": event_type,
                "anomaly_type": "Too Low",
                "description": f"Buy price too low: {price} falls below {LOW_VALUE}",
                "timestamp": current_timestamp,
            }
        elif event_type == "sell" and price >= HIGH_VALUE:
            anomaly = {
                "event_id": payload["user_id"],
                "trace_id": payload["trace_id"],
                "event_type": event_type,
                "anomaly_type": "Too High",
                "description": f"Sell price too high: {price} exceeds {HIGH_VALUE}",
                "timestamp": current_timestamp,
            }

        if anomaly:
            anomalies.append(anomaly)
            logger.info(f"Anomaly detected and added: {anomaly}")
            with open(data_store, 'w') as f:
                json.dump(anomalies, f, indent=4)

        consumer.commit_offsets()

def get_anomalies(anomaly_type):
    logger.info('Get anomalies request received.')

    with open(data_store, 'r') as file:
            anomalies = json.load(file)
    too_high_anomalies = []
    too_low_anomalies = []
    for anomaly in anomalies:
        if anomaly['anomaly_type'] == 'Too High':
            too_high_anomalies.append(anomaly)
        if anomaly['anomaly_type'] == 'Too Low':
            too_low_anomalies.append(anomaly)
            
    if anomaly_type == 'TooHigh':
        response = sorted(too_high_anomalies, key=lambda x: x["timestamp"], reverse=True)
    elif anomaly_type == 'TooLow':
        response = sorted(too_low_anomalies, key=lambda x: x["timestamp"], reverse=True)
    else:
        response = {"anomalies": [], "message": "No anomalies detected."}
    logger.info(f"Response returned: {response}")
    return response, 200

# Connexion app setup
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("BHAVDEEPSINGH_1-OnlineBookstore-1.0.0-resolved.yaml", base_path="/anomaly_detector", strict_validation=True, validate_responses=True)
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if "TARGET_ENV" not in os.environ or os.environ["TARGET_ENV"] != "test":
    CORS(app.app)
    app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":
    t1 = Thread(target=find_anomalies)
    t1.setDaemon(True)
    t1.start()
    app.run(host="0.0.0.0", port=8120)
