import connexion
from connexion import NoContent
import uuid
from sqlalchemy import create_engine,and_
from sqlalchemy.orm import sessionmaker
from base import Base
from book_buy import BookBuy
from book_sell import BookSell
import yaml
import logging
import logging.config
import datetime
import json
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import os

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

user = app_config['datastore']['user']
password = app_config['datastore']['password']
hostname = app_config['datastore']['hostname']
port = app_config['datastore']['port']
db = app_config['datastore']['db']

# DB_ENGINE = create_engine("sqlite:///bookstore.sqlite")

DB_ENGINE = create_engine(
    f'mysql+pymysql://{user}:{password}@{hostname}:{port}/{db}',
    pool_size=10,          
    pool_recycle=500,     
    pool_pre_ping=True     
)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)
logger.info(f"Connecting to db, hostname={hostname}, port={port}")

def book_buy(body):
    """ Receives a new buy event """
    #response = requests.post('http://127.0.0.1:8080/books/buy')
    session = DB_SESSION()
    trace_id = body['trace_id']
    purchase = BookBuy(body['order_id'],
                        body['book_id'],
                        body['user_id'],
                        body['name'],
                        body['price'],
                        body['sold'],
                        body['trace_id'])

    session.add(purchase)
    event_name = 'buy'
    logger.debug(f"Stored event {event_name} request with a trace id of {trace_id}")
    session.commit()
    session.close()

    #return NoContent, response.status_code
    return NoContent, 201


def book_sell(body):
    """ Receives a new sale event """
    session = DB_SESSION()
    trace_id = body['trace_id']
    sale = BookSell(body['book_id'],
                   body['user_id'],
                   body['name'],
                   body['listing_date'],
                   body['price'],
                   body['genre'],
                   body['trace_id']
                   )

    session.add(sale)
    event_name = 'sale'
    logger.debug(f"Stored event {event_name} request with a trace id of {trace_id}")
    session.commit()
    session.close()

    #return NoContent, response.status_code
    return NoContent, 201

def get_books_buy(start_timestamp, end_timestamp):
    """ Gets new book buy events between the start and end timestamps """

    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S")

    results = session.query(BookBuy).filter(
        and_(
            BookBuy.date_created >= start_timestamp_datetime,
            BookBuy.date_created < end_timestamp_datetime))
    
    results_list = []

    for result in results:
        results_list.append(result.to_dict())

    session.close()

    logger.info(f"Query for Book Buy event after {start_timestamp} returns {len(results_list)} results")

    return results_list, 200



def get_books_sell(start_timestamp, end_timestamp):
    """ Gets new book sell events between the start and end timestamps """

    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M:%S")

    results = session.query(BookSell).filter(
        and_(
            BookSell.date_created >= start_timestamp_datetime,
            BookSell.date_created < end_timestamp_datetime))
    
    results_list = []

    for result in results:
        results_list.append(result.to_dict())

    session.close()

    logger.info(f"Query for Book Sell event after {start_timestamp} returns {len(results_list)} results")

    return results_list, 200

def process_messages():
    """ Process event messages """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).
    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)
    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)

        payload = msg["payload"]
        event_type = msg['type']
        if msg["type"] == "buy":
            book_buy(payload)

        elif msg["type"] == "sell":
            book_sell(payload)
        else:
            logger.warning(f"Unknown event type: {event_type}")

        # Commit the new message as being read
        consumer.commit_offsets()
        logger.info(f"Processed and committed offset for message with trace id {payload.get('trace_id')}")



app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("BHAVDEEPSINGH_1-OnlineBookstore-1.0.0-resolved.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(host="0.0.0.0",port=8090)
