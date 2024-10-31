import connexion
from connexion import NoContent
import json
import yaml
import logging
import logging.config
from pykafka import KafkaClient

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

logger = logging.getLogger('basicLogger')

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

def get_books_buy_event(index):
    """ Get book buy event in History """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]

    # Here we reset the offset on start so that we retrieve
    # messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to
    # 100ms. There is a risk that this loop never stops if the
    # index is large and messages are constantly being received!
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,consumer_timeout_ms=1000)
    logger.info("Retrieving buy event at index %d" % index)
    try:
        current_index = 0
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg.get("type") == 'buy':
                if current_index == index:
                    return msg['payload'], 200
                current_index += 1
                
    except:
        logger.error("No more messages found")
    
    logger.error("Could not find buy event at index %d" % index)
    return { "message": "Not Found"}, 404

def get_books_sell_event(index):
    """ Get book sell event in History """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,consumer_timeout_ms=1000)
    logger.info("Retrieving sell event at index %d" % index)
    try:
        current_index = 0
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg.get("type") == 'sell':
                if current_index == index:
                    return msg['payload'], 200
                current_index += 1
                
    except:
        logger.error("No more messages found")
    
    logger.error("Could not find sell event at index %d" % index)
    return { "message": "Not Found"}, 404

def get_stats():
    """ Gets event stats in History """
    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,consumer_timeout_ms=1000)
    num_buy_events = 0
    num_sell_events = 0
    logger.info("Retrieving event stats")
    try:
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            if msg.get("type") == 'sell':
                num_sell_events += 1
            if msg.get("type") == 'buy':
                num_buy_events += 1
        return {
                    "num_buy_events": num_buy_events,
                    "num_sell_events": num_sell_events
                }, 200
            
    except:
        logger.error("No more messages found")
    
    logger.error("Could not event stats")
    return { "message": "Not Found"}, 404


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("BHAVDEEPSINGH_1-OnlineBookstore-1.0.0-resolved.yaml",strict_validation=True,validate_responses=True)

if __name__ == "__main__":
    app.run(port=8110)
