import connexion
from connexion import NoContent
import json
import datetime
import os
import requests
import yaml
import logging
import logging.config
import uuid
from pykafka import KafkaClient
from pykafka.common import OffsetType

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# External Logging Configuration
with open(log_conf_file, 'r') as f:
   log_config = yaml.safe_load(f.read())
   logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

HIGH_VALUE = app_config["thresholds"]["high_value"]
LOW_VALUE = app_config["thresholds"]["low_value"]

def get_anomalies():
    logger.info('Get anomalies request received.')
    logger.info(f"Anomaly thresholds - High Value: {HIGH_VALUE}, Low Value: {LOW_VALUE}")
    hostname = "%s:%d" % (app_config["events"]["hostname"],
                            app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                            reset_offset_on_start=False,
                                            auto_offset_reset=OffsetType.LATEST)
    data_store = app_config['anomaly_detector']['filename']

    # Checking if the file exists
    if not os.path.exists(data_store):
        logger.info(f"Data store {data_store} not found. Creating a new one.")
        with open(data_store, 'w') as file:
            json.dump([], file)

    # Reading the anomalies from the datastore
    with open(data_store, 'r') as file:
            anomalies = json.load(file)    

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)

        payload = msg["payload"]
        event_type = msg['type']
        
        current_timestamp = datetime.datetime.now()
        current_datetime_str = current_timestamp.strftime('%Y-%m-%dT%H:%M:%S')

        # Too High Price
        if payload['price'] >= HIGH_VALUE:
            event_id = payload['user_id']
            trace_id = payload['trace_id']
            anomaly_type = "Too High"
            description = f'The value is too high (Price of {payload['price']} is greater than threshold of {HIGH_VALUE})'
            data = {
            'event_id': event_id,
            'trace_id': trace_id,
            'event_type': event_type,
            'anomaly_type': anomaly_type,
            'description': description,
            'timestamp': current_datetime_str
                    }
            logger.info(f"Anomaly added to database: {data}")
            anomalies.append(data)

        # Too Low Price
        if payload['price'] <= LOW_VALUE:
            event_id = payload['user_id']
            trace_id = payload['trace_id']
            anomaly_type = "Too Low"
            description = f'The value is too low (Price of {payload['price']} is less than threshold of {LOW_VALUE})'
            data = {
            'event_id': event_id,
            'trace_id': trace_id,
            'event_type': event_type,
            'anomaly_type': anomaly_type,
            'description': description,
            'timestamp': current_datetime_str
                    }
            logger.info(f"Anomaly added to database: {data}")
            anomalies.append(data)

        # Updating the datastore with new anomalies
        with open(data_store, 'w') as file:
            json.dump(anomalies, file, indent=4)


    with open(data_store, 'r') as file:
            anomalies = json.load(file)
    response = {"anomalies": sorted(anomalies, key=lambda x: x["timestamp"], reverse=True)}
    logger.info(f"Response returned: {response}")
    return response, 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("BHAVDEEPSINGH_1-OnlineBookstore-1.0.0-resolved.yaml",strict_validation=True,validate_responses=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=8120)
