__author__ = 'v.chernov'

import requests
from pymongo import MongoClient
from multiprocessing import Pool


def db_connection():
    client = MongoClient('mongodb://localhost:27017/')
    base = client.posts_db
    return base.post_collection


def is_link_valid(link_to_test):
    try:
        req = requests.get(link_to_test)
        res = req.text.find('Oops?! Invalid server request!')
        if res == -1:
            return True
        else:
            return False
    except Exception as e:
        print e.message
        return False


def link_collection_generator(start, end):
    return ['http://www.chess.com/tactics/?id=%d' % x for x in xrange(start, end)]


def write_to_mongo(link):
    if is_link_valid(link):
        db = db_connection()
        db.insert({"id": link, "is_valid": True})


if __name__ == "__main__":
    p = Pool(16)
    p.map(write_to_mongo, link_collection_generator(235006, 9999999))
