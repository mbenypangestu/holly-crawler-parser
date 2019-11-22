
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
from utils.tripadvisor_component import TripadvisorComponent


class ReviewService(Database):
    def __init__(self):
        super().__init__()

    def get_all_reviews(self):
        reviews = self.db.review.find()
        return reviews

    def is_review_exist(self, reviewId):
        review = self.db.review.find({'id': reviewId})
        if review.count() > 0:
            return True
        else:
            return False

    def create_many(self, hotel):

        hotelID = hotel['_id']
        hotel_locId = hotel['location_id']

        url = TripadvisorComponent.base_url + str(hotel_locId) + "/reviews"
        next = True

        print(url)

        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.ms': 500,
            'batch.num.messages': 50,
            'default.topic.config': {'acks': 'all'}
        })

        while next == True:
            try:
                response = requests.get(url=url, headers={
                    'X-TripAdvisor-API-Key':
                    '3c7beec8-846d-4377-be03-71cae6145fdc'
                })

                reviews = response.json()['data']
                paging = response.json()['paging']

                if reviews is not None or len(reviews) > 0:
                    print("Count reviews in hotel : ",
                          hotel['name'], " : ", len(reviews))

                    for x, review in enumerate(reviews):
                        is_reviewexist = self.is_review_exist(review["id"])
                        if is_reviewexist:
                            print("Review = ", review['name'],
                                  " is already exist !")
                        else:
                            reviewCreateData = {
                                **review,
                                'hotel': DBRef(collection="hotel", id=hotelID),
                                'hotel_locationID': hotel_locId,
                                'hotel_ObjectId': hotelID,
                                'created_at': datetime.now()
                            }

                            self.db.review.insert_one(reviewCreateData)
                            producer.produce(
                                "reviews", dumps(reviewCreateData))

                            print("Success saving review - ",
                                  reviewCreateData['id'])

                else:
                    print("There is no Reviews data from Hotel ID : ", hotelID)

            except:
                print("Failed to save review from hotel ID : ", hotelID)

            if paging['next'] is not None:
                next = True
                url = paging['next']
            else:
                next = False

        producer.flush()
