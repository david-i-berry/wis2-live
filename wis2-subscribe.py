from datetime import datetime as dt
import json
import paho.mqtt.client as mqtt
import ssl
import logging
import redis
import os
logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)

tictoc = 0
nworkers = 4

# now MQTT functions etc
def on_connect(client, userdata, flags, rc):
    LOGGER.info("connected")
    # subscribe to default topics
    for topic in default_topics:
        LOGGER.info(f"subscribing to {topic}")
        client.subscribe(topic)


def on_message(client, userdata, msg):
    global tictoc
    LOGGER.info(f"message received for topic: {msg.topic}")
    # get time of receipt
    receipt_time = dt.now().isoformat()
    # parse message
    try:
        parsed_message = json.loads(msg.payload)
    except Exception as e:
        LOGGER.error(f"Error parsing JSON message {msg.payload}, {e}")
        return
    topic = msg.topic
    url_ = None
    publish_time = parsed_message['properties'].get('pubtime', dt.now().isoformat())
    publish_time = publish_time[0:19]
    observation_time = parsed_message['properties'].get('datetime', None)
    LOGGER.debug("Checking pubtime")
    if (publish_time is not None) and (observation_time is not None):
        # make sure data is not too old
        publish_datetime = dt.fromisoformat(publish_time.replace("T", " ").replace("Z", ""))  # noqa
        observation_datetime = dt.fromisoformat(observation_time.replace("T", " ").replace("Z", ""))  # noqa
        # Calculate the time difference
        time_difference = publish_datetime - observation_datetime
        if (time_difference.total_seconds() > 3600) and \
                (topic == "cache/a/wis2/ken/ken_met_centre/data/core/weather/surface-based-observations/synop"):  # noqa
            LOGGER.error(f"Data {msg.payload} too old, skipping")
            return
    LOGGER.debug("Parsing message metadata")
    integrity = parsed_message['properties'].get('integrity', None)
    if integrity is not None:
        hash = parsed_message['properties']['integrity']['value']
        hash_method = parsed_message['properties']['integrity']['method']
    else:
        hash = None
        hash_method = None

    for link in parsed_message['links']:
        if (link.get('rel', None) == 'canonical') and \
                (link.get('type', None) in ('application/x-bufr', 'application/octet-stream', 'application/bufr')):
            url_ = link.get('href', None)
    LOGGER.debug("Creating job")
    if url_ is not None:
        job = {
            'key': hash,
            'url': url_,
            'topic': topic,
            'publish_time': publish_time,
            'receipt_time': receipt_time,
            'observation_time': observation_time,
            'hash': hash,
            'hash_method': hash_method
        }
        redis_conn.publish(f"q_{tictoc}", json.dumps(job))
        LOGGER.debug(f"Job published to q_{tictoc}")
        tictoc = tictoc + 1 if tictoc < 3 else 0


# set up redis connection
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
redis_conn = redis.Redis(connection_pool=pool)

# now MQTT
LOGGER.info("Loading MQTT config")
broker = os.getenv('w2gb_broker')
port = int(os.getenv('w2gb_port'))
pwd = os.getenv('w2gb_pwd')
uid = os.getenv('w2gb_uid')
protocol = os.getenv('w2gb_protocol')

default_topics = [
                  'cache/a/wis2/+/+/data/core/+/surface-based-observations/#',
                  'cache/a/wis2/+/data/core/+/surface-based-observations/#',
                  'cache/a/wis2/us-ucsd-scripps-ldl/data/core/weather/experimental/surface-based-observations/#',
                  ]

LOGGER.info("Initialising client")
client = mqtt.Client(transport=protocol)
if port in (443, 8883):
    client.tls_set(ca_certs=None, certfile=None, keyfile=None,
                   cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS,
                   ciphers=None)

client.username_pw_set(uid, pwd)
client.on_connect = on_connect
client.on_message = on_message
LOGGER.debug(f"Connecting to {broker}")
result = client.connect(host=broker, port=port)
LOGGER.info("Looping forever")
client.loop_forever()
