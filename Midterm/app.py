
import flask
import connexion
from connexion import NoContent
import requests
import yaml
import logging
import logging.config
import uuid

num_phone_orders = 0 
num_tablet_orders = 0

logger = logging.getLogger('basicLogger')

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

def order_mobile_phone(body): 
    global num_phone_orders
    logging.debug(f"Received phone order: {body}")

    name = 'Bhavdeep'
    logging.warning(f"Name: {name}")
    num_phone_orders += 1
    return NoContent, 200

def order_tablet(body):
    global num_tablet_orders
    logging.debug(f"Received phone order: {body}")
    student_id = 'A01314938'
    logging.warning(f"Name: {student_id}")
    num_tablet_orders += 1
    return NoContent, 200

def get_order_stats():
    global num_phone_orders, num_tablet_orders
    if num_phone_orders == 0 and num_tablet_orders == 0:
        return 404
    else: 
        return ({
        "num_phones": num_phone_orders,
        "num_tablets": num_tablet_orders
        }), 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi2.yml",strict_validation=True,validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080)
