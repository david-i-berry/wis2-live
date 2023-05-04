#start redis
redis-server /usr/local/etc/redis/redis.conf >& redis.log &
# start wis2 subscriber
python3 wis2-subscribe.py >& subscription.log &