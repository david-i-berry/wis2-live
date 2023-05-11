from datetime import datetime as dt
import json
import paho.mqtt.client as mqtt
import queue
import ssl
import threading
import logging
import redis
import os
from eccodes import (codes_bufr_new_from_file, codes_set, codes_get,
                     codes_release, codes_clone, CODES_MISSING_LONG,
                     CODES_MISSING_DOUBLE, codes_get_native_type,
                     codes_get_array)

import tempfile
import urllib3

# define queue for storing urls to download
processed = {}
last_purge = dt.now()
urlQ = queue.Queue()

LOGGER = logging.getLogger(__name__)


def get_val(handle, key):
    val = None
    try:
        val = codes_get(handle, key)
    except CodesInternalError as e:
        LOGGER.error(f"ECCODES error {e}")
    except Exception as e:
        LOGGER.error(f"Error extracting {key}")
        LOGGER.error(e)
    return val


# python function to extract latitude, longitude, time, elevation from BUFR message
def extract(BUFRFile):
    # open BUFR file
    result = []
    temp = tempfile.NamedTemporaryFile()
    http = urllib3.PoolManager()
    try:
        response = http.request("GET", BUFRFile)
        with open(temp.name, "wb") as fh:
            fh.write(response.data)
    except Exception as e:
        LOGGER.error(f"Error downloading from {BUFRFile}: {e}")
        return None

    with open(temp.name, "rb") as fh:
        messages = True
        handle = codes_bufr_new_from_file(fh)
        if handle is None:
            messages = False
        while messages:
            messages = False
            # unpack data
            codes_set(handle, "unpack", True)
            # get list of descriptors present
            descriptors = codes_get_array(handle, 'expandedDescriptors')
            # check if we have WIGOS identifier
            if 1128 in descriptors:
                useWSI = True
            else:
                LOGGER.error(f"No WSI found in {BUFRFile}, location used as unique ID")
                useWSI = False
            # get number of subsets
            nsubsets = codes_get(handle, "numberOfSubsets")
            # iterate over subsets
            for idx in range(nsubsets):
                codes_set(handle, "extractSubset", idx + 1)
                codes_set(handle, "doExtractSubsets", 1)
                single_subset = codes_clone(handle)
                codes_set(single_subset, "unpack", True)
                # get location, time and elevation
                obs = {
                    "latitude": get_val(single_subset, "#1#latitude"),
                    "longitude": get_val(single_subset, "#1#longitude"),
                    "year": get_val(single_subset, "#1#year"),
                    "month": get_val(single_subset, "#1#month"),
                    "day": get_val(single_subset, "#1#day"),
                    "hour": get_val(single_subset, "#1#hour"),
                    "minute": get_val(single_subset, "#1#minute"),
                    "pressure": get_val(single_subset, "#1#pressure"),
                    "mslp": get_val(single_subset, "#1#pressureReducedToMeanSeaLevel"),
                    "wind_speed": get_val(single_subset, "#1#windSpeed"),
                    "wind_direction": get_val(single_subset, "#1#windDirection"),
                    "air_temperature": get_val(single_subset, "#1#airTemperature"),
                    "dewpoint_temperature": get_val(single_subset, "#1#dewpointTemperature")
                }
                if useWSI:
                    obs["wsi_series"] = get_val(single_subset, '#1#wigosIdentifierSeries')
                    obs["wsi_issuer"] = get_val(single_subset, '#1#wigosIssuerOfIdentifier')
                    obs["wsi_issue_number"] = get_val(single_subset, '#1#wigosIssueNumber')
                    obs["wsi_local_identifier"] = get_val(single_subset, '#1#wigosLocalIdentifierCharacter')
                else:
                    obs["wsi_series"] = 1
                    obs["wsi_issuer"] = 0
                    obs["wsi_issue_number"] = 0
                    x = obs['longitude']
                    y = obs['latitude']
                    obs["wsi_local_identifier"] = f"POINT({x} {y})"
                LOGGER.debug(f"{BUFRFile}: {json.dumps(obs)}")
                result.append(obs)
                codes_release(single_subset)
            codes_release(handle)
            handle = codes_bufr_new_from_file(fh)
            if handle is not None:
                messages = True
    return result


# define worker to do the downloads
def downloadWorker():
    while True:
        LOGGER.info(f"Messages in queue: {urlQ.qsize()}")
        job = urlQ.get() # get latest job from queue
        key = job["key"]
        if key in processed:
            LOGGER.debug("skipping, file already processed")
            continue
        url_ = job["url"]
        # process the data
        subsets = None
        try:
            subsets = extract(url_)
        except Exception as e:
            timestamp = dt.fromisoformat(job['receipt_time'])
            zscore = dt.timestamp(timestamp)
            redis.zadd("error", {url_: zscore})
            LOGGER.error(f"Error extracting data from {url_}")

        processed[key] = dt.now()
        if subsets is not None:
            for subset in subsets:
                if subset['wsi_local_identifier'] != "":
                    wigos_id = f"{subset['wsi_series']}-{subset['wsi_issuer']}-{subset['wsi_issue_number']}-{subset['wsi_local_identifier']}"
                    # first use zadd
                    timestamp = dt.fromisoformat( job['receipt_time'])
                    zscore = dt.timestamp(timestamp)
                    redis.zadd("default", {wigos_id: zscore})
                    redis.set(wigos_id, json.dumps(subset))
                else:
                    LOGGER.error(f"{url_}: wsi is missing")

        urlQ.task_done()


# now MQTT functions etc
def on_connect(client, userdata, flags, rc):
    LOGGER.info("connected")
    # subscribe to default topics
    for topic in default_topics:
        LOGGER.info(f"subscribing to {topic}")
        client.subscribe(topic)


def on_message(client, userdata, msg):
    global processed
    global last_purge
    LOGGER.info("message received")
    # get time of receipt
    receipt_time = dt.now().isoformat()
    # parse message
    try:
        parsed_message = json.loads(msg.payload)
    except:
        LOGGER.error(f"Error parsing JSON message {msg.payload}")
        return
    topic = msg.topic
    url_ = None
    #LOGGER.error(parsed_message)
    publish_time = parsed_message['properties'].get('pubtime', dt.now().isoformat())
    observation_time = parsed_message['properties'].get('datetime', None)
    integrity = parsed_message['properties'].get('integrity', None)
    if integrity is not None:
        hash = parsed_message['properties']['integrity']['value']
        hash_method = parsed_message['properties']['integrity']['method']
    else:
        hash = None
        hash_method = None

    for link in parsed_message['links']:
        if (link.get('rel', None) == 'canonical') and (link.get('type', None) == 'application/x-bufr'):
            url_ = link.get('href', None)

    if (url_ is not None) and (url_ not in processed):
        # check whether we have processed already
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
        urlQ.put(job)

    # check whether we need to purge the processed list
    time_now = dt.now()
    time_since_purge = time_now - last_purge
    if time_since_purge.total_seconds() > 300:
        processed = purge(processed, time_now)
        last_purge = dt.now()

def purge(p, t):
    return {k:v for k, v in p.items() if (v-t).total_seconds() > 300}

# start worker in the background
LOGGER.info("Spawning worker")
threading.Thread(target=downloadWorker, daemon=True).start()


# set up redis connection
pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
redis = redis.Redis(connection_pool=pool)

LOGGER.info("Loading MQTT config")
broker = os.getenv('w2gb_broker')
port = int(os.getenv('w2gb_port'))
pwd = os.getenv('w2gb_pwd')
uid = os.getenv('w2gb_uid')
protocol = os.getenv('w2gb_protocol')

default_topics = [
                  'cache/a/wis2/+/+/+/+/+/+/synop'
                  ]

LOGGER.info("Initialising client")
client = mqtt.Client(transport="websockets")
client.tls_set(ca_certs=None, certfile=None, keyfile=None,
               cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS,
               ciphers=None)
client.username_pw_set(uid, pwd)
client.on_connect = on_connect
client.on_message = on_message
LOGGER.info("Connecting")
result = client.connect(host=broker, port=port)
LOGGER.info("Looping forever")
client.loop_forever()
