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

from .database_service import Database
from .review_service import ReviewService
from utils.tripadvisor_component import TripadvisorComponent


class HotelService(Database):
    def __init__(self):
        super().__init__()

    def get_all_hotels(self):
        hotels = self.db.hotel.find()
        return hotels

    def is_hotel_exist(self, loc_hotel_id):
        hotel = self.db.hotel.find_one({'location_id': loc_hotel_id})
        if hotel != None:
            return True
        else:
            return False

    def create_many(self, loc):
        reviewService = ReviewService()

        loc_id = loc['location_id']
        url = TripadvisorComponent.base_url + str(loc_id) + "/hotels"
        next = True

        print(url)

        while next == True:
            try:
                response = requests.get(url=url, headers={
                    'X-TripAdvisor-API-Key':
                    '3c7beec8-846d-4377-be03-71cae6145fdc'
                })

                hotels = response.json()['data']
                paging = response.json()['paging']

                if hotels is not None or len(hotels) > 0:
                    print("Count hotels in location : ", len(hotels), "\n")

                    for i, hotel in enumerate(hotels):
                        is_hotelexist = self.is_hotel_exist(
                            hotel['location_id'])

                        if is_hotelexist == True:
                            print("Hotel = ", hotel['name'],
                                  " is already exist !")
                        else:
                            hotelCreateData = {
                                **hotel,
                                'location': DBRef(collection="location", id=loc["_id"]),
                                'locationID': loc_id,
                                'location_ObjectId': loc['_id'],
                                'created_at': datetime.now()
                            }

                            result_id = (self.db.hotel.insert_one(
                                hotelCreateData)).inserted_id
                            print(result_id)

                            hotelByObjectID = self.db.hotel.find_one(
                                {"_id": result_id})

                            reviewService.create_many(hotelByObjectID)

                else:
                    print("There is no hotels data from location ID : ", loc_id)

                if paging['next'] != None:
                    next = True
                    url = paging['next']
                else:
                    next = False

            except:
                print("Failed to save hotel from location ID : ", loc_id)
