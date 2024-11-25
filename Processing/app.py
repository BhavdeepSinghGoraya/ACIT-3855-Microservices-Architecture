import connexion
from connexion import NoContent
import uuid
from sqlalchemy import create_engine,and_
from sqlalchemy.orm import sessionmaker
import yaml
import logging
import logging.config
import datetime
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import os
import json
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

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


def get_stats():
    logger.info(f'Get stats request has started')
    datastore = app_config['datastore']['filename']
    try: 
        with open(datastore, 'r') as file:
            data = json.load(file)
    except FileNotFoundError: 
        logger.error(f'Statistics do not exis')
        return {"message": "Statistics do not exist"}, 404
    response_data = {
        'num_buy_events': data['num_buy_events'],
        'max_buy_price': data['max_buy_price'],
        'num_sell_events': data['num_sell_events'],
        'max_sell_price': data['max_sell_price'],
        'last_updated': data['last_updated']
        }
    
    logger.debug(f"Statistics response: {response_data}")
    logger.info(f'Get stats request has completed')

    return response_data, 200

def populate_stats():
    """ Periodically update stats """

    logger.info(f'Periodic processing has started')

    data_store = app_config['datastore']['filename']
    if os.path.exists(data_store):
        with open(data_store, 'r') as file:
                data = json.load(file)
    else: 
        data = {
        'num_buy_events': 0,
        'max_buy_price': 0,
        'num_sell_events': 0,
        'max_sell_price': 0,
        'last_updated': '2024-10-03T11:03:00'
        }

    current_timestamp = datetime.datetime.now()
    current_datetime_str = current_timestamp.strftime('%Y-%m-%dT%H:%M:%S')
    start_timestamp = data['last_updated']
    end_timestamp = current_datetime_str


    app_url = app_config['eventstore']['url']
    buy_url = f'{app_url}/books/buy?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}'
    response = requests.get(buy_url)

    if response.status_code == 200:
        result_list = response.json()
        num_buy_events = len(result_list)
        max_buy_price = max(result['price'] for result in result_list) if result_list else data['max_buy_price']
    else:
        logger.error(f"Failed to get buy events. Status code: {response.status_code}")

    sell_url = f'{app_url}/books/sell?start_timestamp={start_timestamp}&end_timestamp={end_timestamp}'
    response = requests.get(sell_url)
    if response.status_code == 200:
        result_list = response.json()
        num_sell_events = len(result_list)
        max_sell_price = max(result['price'] for result in result_list) if result_list else data['max_sell_price']
    else:
        logger.error(f"Failed to get sell events. Status code: {response.status_code}")

    num_of_events = num_buy_events + num_sell_events
    logger.info(f'Total {num_of_events} events received')


    data['num_buy_events'] += num_buy_events
    data['max_buy_price'] = max_buy_price
    data['num_sell_events'] += num_sell_events
    data['max_sell_price'] = max_sell_price
    data['last_updated'] = current_datetime_str

    with open(data_store, 'w') as file:
        json.dump(data, file)
    
    logger.debug(f"Updated stats: {data}")


    

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                   seconds=app_config['scheduler']['period_sec'])
    sched.start()

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("BHAVDEEPSINGH_1-OnlineBookstore-1.0.0-resolved.yaml", strict_validation=True, validate_responses=True)
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    # run our standalone gevent server
    init_scheduler()
    app.run(host="0.0.0.0",port=8100)
