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

EVENT_FILE = "event.json"
MAX_EVENTS = 5

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

# Creating KafkaClient
try:
    server = app_config['events']['hostname']
    port = app_config['events']['port']
    client = KafkaClient(hosts=f'{server}:{port}')
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer()
except:
    logger.error(f'Unable to create connection with Kafka client.')

    
def book_buy(body):
    trace_id = str(uuid.uuid4())
    body['trace_id'] = trace_id
    logger.info(f'Received event buy request with a trace id of {trace_id}')
    msg = { "type": "buy",
            "datetime" :
                datetime.datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S"),
                "payload": body }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info(f'Returned event buy response (Id: {trace_id}) with status 201')
    return NoContent, 201

def book_sell(body):
    trace_id = str(uuid.uuid4())
    body['trace_id'] = trace_id
    logger.info(f'Received event sell request with a trace id of {trace_id}')
    msg = { "type": "sell",
            "datetime" :
                datetime.datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S"),
                "payload": body }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info(f'Returned event sell response (Id: {trace_id}) with status 201')
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("BHAVDEEPSINGH_1-OnlineBookstore-1.0.0-resolved.yaml",strict_validation=True,validate_responses=True)

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=8080)
