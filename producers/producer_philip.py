"""
producer_philip.py

Stream JSON data to a file and - if available - a Kafka topic.

Example JSON message
{"machine_id": 2,
"timestamp": 1738803338.650943,
"temperature": 50.56,
"rpm": 4395,
"widgets_produced": 74,
"conveyor_speed": 1.96,
"product_quality": "Good",
"error_code": "E202"
}


Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import os
import pathlib
import random
import sys
import time
from datetime import datetime

# import external modules
import faker 
from kafka import KafkaProducer

# import from local modules
import utils.utils_config as config
from utils.utils_producer import verify_services, create_kafka_topic
from utils.utils_logger import logger

#####################################
# Stub Sentiment Analysis Function
#####################################


def assess_sentiment(text: str) -> float:
    """
    Stub for sentiment analysis.
    Returns a random float between 0 and 1 for now.
    """
    return round(random.uniform(0, 1), 2)


#####################################
# Define Message Generator
#####################################

fake = faker.Faker()

def generate_messages():
    """
    Generate a stream of JSON messages.
    """
    
    # Create JSON message
    machine_id = random.randint(1, 100)  # Example machine_id range
    json_message = {
        'machine_id': machine_id,
        'timestamp': time.time(),  # Unix timestamp
        'temperature': round(random.choices(
            population=[random.uniform(50, 60), random.uniform(20, 49.99), random.uniform(60.01, 80)],
            weights=[0.95, 0.025, 0.025],
            k=1)[0], 2),
        'rpm': random.choices(
            population=[random.randint(4000, 5000), random.randint(1000, 3999)],
            weights=[0.95, 0.05],
            k=1
        )[0],
        'widgets_produced': random.randint(0, 200),
        'conveyor_speed': round(random.uniform(1.5, 2.0), 2),  # Meters per second
        'product_quality': "Good" if random.random() < 0.95 else "Bad",  # 95% chance of being "Good"
        'error_code': random.choice([None, "E101", "E202", "E303"]),  # Null is no error
    }

    yield json_message


#####################################
# Define Main Function
#####################################


def main() -> None:

    logger.info("Starting Producer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read required environment variables.")

    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        topic: str = config.get_kafka_topic()
        kafka_server: str = config.get_kafka_broker_address()
        live_data_path: pathlib.Path = config.get_live_data_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete the live data file if exists to start fresh.")

    try:
        if live_data_path.exists():
            live_data_path.unlink()
            logger.info("Deleted existing live data file.")

        logger.info("STEP 3. Build the path folders to the live data file if needed.")
        os.makedirs(live_data_path.parent, exist_ok=True)
    except Exception as e:
        logger.error(f"ERROR: Failed to delete live data file: {e}")
        sys.exit(2)

    logger.info("STEP 4. Try to create a Kafka producer and topic.")
    producer = None

    try:
        verify_services()
        producer = KafkaProducer(
            bootstrap_servers=kafka_server,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        logger.info(f"Kafka producer connected to {kafka_server}")
    except Exception as e:
        logger.warning(f"WARNING: Kafka connection failed: {e}")
        producer = None

    if producer:
        try:
            create_kafka_topic(topic)
            logger.info(f"Kafka topic '{topic}' is ready.")
        except Exception as e:
            logger.warning(f"WARNING: Failed to create or verify topic '{topic}': {e}")
            producer = None

    logger.info("STEP 5. Generate messages continuously.")
    try:
        NUM_MACHINES = 5
        SENSORS_PER_MACHINE = 3

        while True:
            for machine_id in range(1, NUM_MACHINES + 1):
                for _ in range(SENSORS_PER_MACHINE):
                    message = next(generate_messages())
                    message['machine_id'] = machine_id  # Ensure the message has the correct machine_id
                    logger.info(f"STEP 4. Generated message: {message}")

                    # Write to file
                    with live_data_path.open("a") as f:
                        f.write(json.dumps(message) + "\n")
                        logger.info(f"STEP 4a Wrote message to file: {message}")

                    # Send to Kafka if available
                    if producer:
                        producer.send(topic, value=message)
                        logger.info(f"STEP 4b Sent message to Kafka topic '{topic}': {message}")

                    time.sleep(interval_secs)


    except KeyboardInterrupt:
        logger.warning("WARNING: Producer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed.")
        logger.info("TRY/FINALLY: Producer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
