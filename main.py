
import pprint
import requests
import json
import logging

from bson import Binary, Code
from bson.json_util import dumps
from bson.dbref import DBRef

from datetime import datetime
from time import sleep

from confluent_kafka import Producer
from confluent_kafka import KafkaException
from confluent_kafka import KafkaError

from kafka import KafkaProducer

from mongoengine import connect
from pymongo import MongoClient

from service.location_service import LocationService
from service.hotel_service import HotelService
from service.database_service import Database


class Spider(Database):
    def __init__(self):
        super().__init__()
        self.crawl()

    def crawl(self):
        locationService = LocationService()
        hotelService = HotelService()

        locations = locationService.get_all_locations()
        for i, loc in enumerate(locations):
            loc_id = loc['location_id']
            print("\n", i+1, ". ", loc_id, " - ", loc['name'], "\n")

            hotelService.create_many(loc)


def handle_dr(err, msg):
    assert err is not None
    assert err.code() == KafkaError._MSG_TIMED_OUT
    assert "Message timed out" in err.str()


if __name__ == "__main__":
    print("---------------- Start crawling data ! ------------------")
    Spider()
