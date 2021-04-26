from django.db import models
# from ../dataaccess import entity_management as entity_management
# Create your models here.

from accessdata import entity_management
from accessdata import constants
import json

from bson.objectid import ObjectId

def create_product(json_to_insert):
    try:
        result = entity_management.insert_collection_data(constants.DB_COLLECTION['products'],json_to_insert)
        return result

    except Exception as error:
        # LOGGER.exception(error)
        raise error

def get_all_products():
    try:
        result = json.loads(entity_management.fetch_collection_data(constants.DB_COLLECTION['products']))
        return result
    except Exception as error:
        raise error

def delete_product(id):
    try:
        result = entity_management.delete_collection_data(id,constants.DB_COLLECTION['products'])
        return result
    except Exception as error:
        raise error

def update_product(filter_data,json_to_insert):
    try:
        collection = constants.DB_COLLECTION['products']
        method = constants.DB_METHOD_SET
        model, result = entity_management.update_collection_data(collection, filter_data, json_to_insert, method)
        return result

    except Exception as error:
        raise error

def get_product(id):
    try:
        collection = constants.DB_COLLECTION['products']
        filter_data = {
            '_id': ObjectId(id)
        }
        result = entity_management.fetch_data_multiple_keys(collection,filter_data)

        print("result",result)
        return result
    except Exception as error:
        raise error

def get_product_by_name(product_name):
    try:
        collection = constants.DB_COLLECTION['products']
        filter_data = {"productName" : {'$regex' : '.*' + product_name + '.*'}}

        result = entity_management.fetch_data_multiple_keys(collection,filter_data)
        
        print("result",result)
        return result
        
    except Exception as error:
        raise error