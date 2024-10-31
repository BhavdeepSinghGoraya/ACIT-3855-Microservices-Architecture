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

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

logger = logging.getLogger('basicLogger')

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

def book_buy(body):
    # message = f'Book {body["book_id"]} named {body["name"]} was brought for {body["price"]}'
    # api_log('num_buying',message)
    trace_id = str(uuid.uuid4())
    body['trace_id'] = trace_id
    url = app_config['buy']['url']
    headers = {
        'Content-Type': 'application/json'
    }
    logger.info(f'Received event buy request with a trace id of {trace_id}')
    #response = requests.post(url,json=body,headers=headers)
    server = app_config['events']['hostname']
    port = app_config['events']['port']
    client = KafkaClient(hosts=f'{server}:{port}')
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer()
    msg = { "type": "buy",
            "datetime" :
                datetime.datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S"),
                "payload": body }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info(f'Returned event buy response (Id: {trace_id}) with status 201')
    return NoContent, 201

# def api_log(event,message):
#     if os.path.exists(EVENT_FILE):
#         with open(EVENT_FILE, 'r') as file:
#             data = json.load(file)
#         current_timestamp = datetime.datetime.now()
#         current_datetime_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
#         event_data = {
#             "msg_data": message,
#             "received_timestamp": current_datetime_str
#         }
#         for key in data:
#             if key == event:
#                 data[key] += 1

#         for key in data:
#             if event == 'num_buying' and key == 'num_buying':
#                 data['recent_buy'].insert(0,event_data)
#                 data['recent_buy'] = data['recent_buy'][:MAX_EVENTS]
#             if event == 'num_selling' and key == 'num_selling':
#                 data['recent_sell'].insert(0,event_data)
#                 data['recent_sell'] = data['recent_sell'][:MAX_EVENTS]
#         with open(EVENT_FILE, 'w') as file:
#             json.dump(data, file, indent=4)
#     else:
#         starter_data ={
#             'num_buying':0,
#             'recent_buy':[],
#             'num_selling':0,
#             'recent_sell':[]
#         }
#         with open(EVENT_FILE,'w') as file:
#             json.dump(starter_data,file,indent=4)

def book_sell(body):
    # message = f'Book {body["book_id"]} named {body["name"]} was sold for {body["price"]}'
    # api_log('num_selling',message)
    trace_id = str(uuid.uuid4())
    body['trace_id'] = trace_id
    print(body)
    url = app_config['sell']['url']
    print(url)
    headers = {
        'Content-Type': 'application/json'
    }
    logger.info(f'Received event sell request with a trace id of {trace_id}')
    #response = requests.post(url,json=body,headers=headers)
    server = app_config['events']['hostname']
    port = app_config['events']['port']
    client = KafkaClient(hosts=f'{server}:{port}')
    topic = client.topics[str.encode(app_config['events']['topic'])]
    producer = topic.get_sync_producer()
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
    app.run(port=8080)
