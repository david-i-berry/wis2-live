import argparse
from datetime import datetime as dt
import json
import threading
import logging
import redis
from eccodes import (codes_bufr_new_from_file, codes_set, codes_get,
                     codes_release, codes_clone, CODES_MISSING_LONG,
                     CODES_MISSING_DOUBLE, codes_get_native_type,
                     codes_get_array, CodesInternalError)

import tempfile
import urllib3

logging.basicConfig(level=logging.INFO)
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
    #http = urllib3.PoolManager(cert_reqs='CERT_NONE')
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
            LOGGER.error(f"Handle is none: {BUFRFile}")
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
                LOGGER.warning(f"No WSI found in {BUFRFile}, location used as unique ID")
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

def downloadWorker(redis_client, redis_conn):

    for msg in redis_client.listen():
        job = msg
        if job.get("type", None) == "subscribe":
            print("new sub")
            continue
        job = json.loads(msg['data'].decode('utf-8'))
        key = job["key"]
        if redis_conn.get(key) != None:
            LOGGER.debug(f"Skipping job {key}, data already processed")
            continue

        url_ = job["url"]
        # process the data
        subsets = None
        download_error = False
        try:
            subsets = extract(url_)
        except Exception as e:
            download_error = True
            LOGGER.error(f"Error extracting data from {url_}: {e}")

        if subsets is not None:
            for subset in subsets:
                if subset['wsi_local_identifier'] != "":
                    wigos_id = f"{subset['wsi_series']}-{subset['wsi_issuer']}-{subset['wsi_issue_number']}-{subset['wsi_local_identifier']}"  # noqa
                    # first use zadd
                    timestamp = dt.fromisoformat( job['receipt_time'])
                    zscore = dt.timestamp(timestamp)
                    LOGGER.debug(f"zscore: {zscore}, {timestamp}")
                    redis_conn.zadd("default", {wigos_id: zscore})
                    redis_conn.set(wigos_id, json.dumps(subset), ex = 86400)
                else:
                    LOGGER.warning(f"{url_}: wsi is missing")

        if not download_error:
            zscore = dt.timestamp(dt.now())
            redis_conn.zadd("processed", {key: zscore})
            redis_conn.set(key, dt.now().isoformat(), ex = 3600)  # noqa

def main(args):
    q_number = args.q
    # set up redis connection for DB
    pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
    redis_conn = redis.Redis(connection_pool=pool)
    # set up subscription for new jobs
    r = redis.Redis(
        connection_pool=pool,
        decode_responses=True
    )
    redis_sub = r.pubsub()
    redis_sub.subscribe(f'q_{q_number}')

    redis_thread = threading.Thread(target=downloadWorker, args=(redis_sub,redis_conn))

    LOGGER.info("Starting thread")
    redis_thread.start()

    # Wait for the threads to finish
    redis_thread.join()
    LOGGER.info("Ending")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processor for wis2 subs")
    # Add command-line arguments using add_argument method
    parser.add_argument("q", help="Processor number")
    args = parser.parse_args()
    main(args)