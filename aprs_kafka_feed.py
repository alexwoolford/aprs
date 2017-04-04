#!/usr/bin/env python
import aprslib
import logging
import json
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)


class AprsKafkaFeed:

    def __init__(self, callsign, kafka_bootstrap_servers):
        self.callsign = callsign
        self.kafka_producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    def callback(self, packet):
        if packet.get('latitude') and packet.get('longitude') and packet.get('weather') and packet.get('timestamp') and packet.get('from'):
            weather = packet['weather']
            weather['latitude'] = packet['latitude']
            weather['longitude'] = packet['longitude']
            weather['timestamp'] = packet.get('timestamp')
            weather['from'] = packet.get('from')
            self.kafka_producer.send('weather', json.dumps(weather))

        if packet.get('latitude') and packet.get('longitude') and packet.get('timestamp') and packet.get('speed') and packet.get('course') and packet.get('altitude') and packet.get('from'):
            speed = dict()
            speed['latitude'] = packet.get('latitude')
            speed['longitude'] = packet.get('longitude')
            speed['timestamp'] = packet.get('timestamp')
            speed['speed'] = packet.get('speed')
            speed['course'] = packet.get('course')
            speed['altitude'] = packet.get('altitude')
            speed['from'] = packet.get('from')
            self.kafka_producer.send('speed', json.dumps(speed))

    def run(self):
        aprs = aprslib.IS(self.callsign)
        aprs.connect()
        aprs.consumer(self.callback)

    def __del__(self):
        self.kafka_producer.close()

if __name__ == "__main__":
    aprs_kafka_feed = AprsKafkaFeed(callsign='w9xyz', kafka_bootstrap_servers=['hdf2.woolford.io:6667'])
    aprs_kafka_feed.run()
