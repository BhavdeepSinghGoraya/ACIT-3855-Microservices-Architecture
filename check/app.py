import connexion
import logging
import logging.config
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import os
import json
import yaml
from requests.exceptions import Timeout, ConnectionError

# Determine the environment and load appropriate configuration files
if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

# Load application configuration
with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# Load logging configuration
with open(log_conf_file, 'r') as f:
   log_config = yaml.safe_load(f.read())
   logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

# Log configuration files used
logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

# Extract configuration values
RECEIVER_URL = app_config['eventstore']['receiver']
STORAGE_URL = app_config['eventstore']['storage']
PROCESSING_URL = app_config['eventstore']['processing']
ANALYZER_URL = app_config['eventstore']['analyzer']
TIMEOUT = app_config['threshold']['timeout']
DATA_STORE = app_config['data_store']['filename']

def check_services():
    """Check the health of various services and log their status periodically."""

    logger.info("Starting health check for all services.")

    # Ensure datastore exists
    if not os.path.exists(DATA_STORE):
        logger.info(f"Creating data store: {DATA_STORE}")
        with open(DATA_STORE, 'w') as f:
            json.dump([], f)

    # Check Receiver service
    receiver_status = "Unavailable"
    try:
        logger.info("Checking Receiver service health.")
        response = requests.get(RECEIVER_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            receiver_status = "Healthy"
            logger.info("Receiver service is healthy.")
        else:
            logger.warning("Receiver service returned a non-200 response.")
    except (Timeout, ConnectionError):
        logger.error("Receiver service is not available.")

    # Check Storage service
    storage_status = "Unavailable"
    try:
        logger.info("Checking Storage service health.")
        response = requests.get(STORAGE_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            storage_json = response.json()
            storage_status = f"Storage has {storage_json['num_buy_events']} Buy Events and {storage_json['num_sell_events']} Sell events"
            logger.info("Storage service is healthy.")
        else:
            logger.warning("Storage service returned a non-200 response.")
    except (Timeout, ConnectionError):
        logger.error("Storage service is not available.")

    # Check Analyzer service
    analyzer_status = "Unavailable"
    try:
        logger.info("Checking Analyzer service health.")
        response = requests.get(ANALYZER_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            analyzer_json = response.json()
            analyzer_status = f"Analyzer has {analyzer_json['num_buy_events']} Buy Events and {analyzer_json['num_sell_events']} Sell events"
            logger.info("Analyzer service is healthy.")
        else:
            logger.warning("Analyzer service returned a non-200 response.")
    except (Timeout, ConnectionError):
        logger.error("Analyzer service is not available.")

    # Check Processing service
    processing_status = "Unavailable"
    try:
        logger.info("Checking Processing service health.")
        response = requests.get(PROCESSING_URL, timeout=TIMEOUT)
        if response.status_code == 200:
            processing_json = response.json()
            processing_status = f"Processing has {processing_json['num_buy_events']} Buy Events and {processing_json['num_sell_events']} Sell events"
            logger.info("Processing service is healthy.")
        else:
            logger.warning("Processing service returned a non-200 response.")
    except (Timeout, ConnectionError):
        logger.error("Processing service is not available.")

    # Log the overall status and write to the datastore
    status = {
        "receiver": receiver_status,
        "storage": storage_status,
        "processing": processing_status,
        "analyzer": analyzer_status
    }

    logger.info("Writing service status to datastore.")
    with open(DATA_STORE, "w") as file:
        json.dump(status, file, indent=2)
    logger.info("Health check completed.")

def get_checks():
    """Retrieve the last recorded health check statuses."""
    logger.info("Fetching the latest health check status.")

    if os.path.exists(DATA_STORE):
        with open(DATA_STORE) as file:
            data = json.load(file)
        logger.info("Successfully retrieved health check status.")
        return data, 200
    
    logger.error("Status file not found.")
    return {"error": "Status file not found"}, 404

def init_scheduler():
    """Initialize and start the scheduler for periodic health checks."""
    logger.info("Initializing scheduler for periodic health checks.")

    sched = BackgroundScheduler(daemon=True)
    sched.add_job(check_services,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()
    logger.info("Scheduler started successfully.")

# Initialize the Connexion app
app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", base_path="/check", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    # Start the application
    logger.info("Starting the Flask application.")
    init_scheduler()
    app.run(host="0.0.0.0", port=8130)
    logger.info("Flask application is running.")