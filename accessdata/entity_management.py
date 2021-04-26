import base64
import datetime
import json
import logging
import os
# import cv2

# import bcrypt
from typing import Dict, List, Any

from bson import json_util
import bson
from bson.objectid import ObjectId

from django.conf import settings
from pymongo import MongoClient, ReturnDocument

from . import external_services
import os
from os import path

import collections
from collections import OrderedDict

import constants as constant_utils

LOGGER = logging.getLogger(__name__)

client = MongoClient(external_services.MONGO_HOST, external_services.MONGO_PORT)
db = client[external_services.MONGO_DB]
user_db = client[external_services.MONGO_USER_DB]
tenant_db = client[external_services.MONGO_TENANT_DB]

# TO retrieve collection
def get_table_cursor(collection):
    try:
        global db
        table = db[collection]

        return table

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

def get_user_table_cursor(collection):
    try:
        global user_db
        table = user_db[collection]

        return table

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

def get_tenant_table_cursor(collection):
    try:
        global tenant_db
        table = tenant_db[collection]

        return table

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

def get_table_data(table, app_id=None, field=None, parameter=None, recent20=False, regex=False):
    try:
        if regex:
            parameter = {"$regex":parameter}

        if field:
            if ObjectId.is_valid(parameter) and field != 'rule':
                parameter = ObjectId(parameter)
            if recent20:
                user = table.find({field: parameter}).limit(20).sort([('$natural', -1 )])
            else:
                user = table.find({field: parameter})
        elif app_id:
            if recent20:
                user = table.find({"appID": ObjectId(app_id)}).limit(20).sort([('$natural', -1 )])
            else:
                user = table.find({"appID": ObjectId(app_id)})        
        else:
            user = table.find({})

        return user

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

#converts string document to object type to push into database
def convert_str_to_obj_data(json_to_insert, app_id=None):
    try:
        if "_id" in json_to_insert:
            del json_to_insert["_id"]
        if app_id is not None:
            json_to_insert["appID"] = ObjectId(app_id)

        for str_data in json_to_insert:
            if str_data in ["filesList", "validatedFileList", "predictedFileList", "filesUsedforModel"]:
                file_list = json_to_insert[str_data]

                if isinstance(file_list, list):
                    str_file_list = []
                    for file in file_list:
                        if isinstance(file, str):
                            file = ObjectId(file)
                        str_file_list.append(file)
                else:
                    if isinstance(file_list, str):
                        str_file_list = ObjectId(file_list)

                json_to_insert[str_data] = str_file_list

            elif str_data == "listOfApps":
                for app in json_to_insert[str_data]:
                    for sub_app in app:
                        if (sub_app == "appID"):
                            app[sub_app] = ObjectId(app[sub_app])
                        elif sub_app == "users":
                            for content in app[sub_app]:
                                for content_data in content:
                                    if content_data == "createdDateTime":
                                        content[content_data] = datetime.datetime.strptime(content[content_data], "%Y-%m-%d %H:%M:%S.%f")

            elif ObjectId.is_valid(json_to_insert[str_data]) and str_data != "rule" and isinstance(
                    json_to_insert[str_data], str):
                json_to_insert[str_data] = ObjectId(json_to_insert[str_data])

        return json_to_insert

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

#converts object type to string for UI
def convert_obj_to_str_data(user, key_to_be_removed=None, isID=False):
    try:
        user_load = {"list": []}
        for data in user:
            for str_data in data:
                if isinstance(data[str_data], datetime.datetime):
                    data[str_data] = str(data[str_data])

                if str_data in ["form_selected"]:
                    temp_dict = data[str_data]
                    for k,v in temp_dict.items():
                        if isinstance(v, bson.objectid.ObjectId):
                            temp_dict[k] = str(v)
                    data[str_data] = temp_dict    

                # if str_data in list_of_data:
                if str_data in ["filesList", "validatedFileList", "predictedFileList", "filesUsedforModel"]:
                    file_list = data[str_data]
                    str_file_list = []
                    for file in file_list:
                        file = str(file)
                        str_file_list.append(file)
                    data[str_data] = str_file_list

                elif str_data == "listOfApps":
                    for app in data[str_data]:
                        for sub_app in app:
                            if (sub_app == "appID" or sub_app == "tenantId"):
                                app[sub_app] = str(app[sub_app])
                            elif sub_app == "users":
                                for content in app[sub_app]:
                                    for content_data in content:
                                        if isinstance(content[content_data], datetime.datetime):
                                            content[content_data] = str(content[content_data])

                elif ObjectId.is_valid(data[str_data]) and str_data != "rule":
                    data[str_data] = str(data[str_data])

            if key_to_be_removed is not None:
                for key in key_to_be_removed:
                    if key in data: del data[key]

            user_load["list"].append(data)

        return json_util.dumps(user_load["list"]) if isID == False else json_util.dumps(user_load["list"][0])

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


'''
fetch data according to field.
if field is _id, returns document dictionary, otherwise will retuen a list od dictionary.
'''
def fetch_collection_data(collection, app_id=None, field=None, parameter=None, key_to_be_removed=None, recent20=False, regex=False):
    try:
        isID = False
        table = get_table_cursor(collection)
        user = get_table_data(table, app_id, field, parameter, recent20, regex=regex)

        if field == "_id":
            isID = True
        if user.count() == 0 and isID ==True:
            return json_util.dumps({})
        elif user.count() == 0 and isID ==False:
            return json_util.dumps([])
        converted_user_data = convert_obj_to_str_data(user, key_to_be_removed, isID) 
        return converted_user_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

'''
fetches collection documents based on multiple key  or complex queries
'''
def fetch_data_multiple_keys(collection, filter_data= {}, key_to_be_removed=None):
    try:
        isID = False
        table = get_table_cursor(collection)
        user = table.find(filter_data)
        converted_user_data = convert_obj_to_str_data(user, key_to_be_removed, isID)
        return converted_user_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


'''
Insert document to collection
'''
def insert_collection_data(collection, json_to_insert, app_id=None):
    try:
        json_dictionary = dict()
        table = get_table_cursor(collection)
        json_data = convert_str_to_obj_data(json_to_insert, app_id)
        user_details = table.insert_one(json_data)
        json_dictionary['_id'] = str(user_details.inserted_id)
        json_dictionary['message'] = constant_utils.ENTITY_CREATION

        return json_dictionary

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        if isinstance(error,bson.errors.InvalidDocument):
            try:
                json_data = OrderedDict([(k.replace('.'," "), v) if "." in k else (k, v) for k, v in json_data.items()])
                user_details = table.insert_one(json_data)
                json_dictionary['_id'] = str(user_details.inserted_id)
                json_dictionary['message'] = constant_utils.ENTITY_CREATION
            except Exception as error:
                LOGGER.exception(error)
                raise error
        else:
            raise error


'''
Update single document to collection and returns the updated document
                                if return_document_bool is set as true
'''
def update_collection_data(collection, filter_data, save_data, method, return_document_bool=False, app_id=None):
    try:
        result = dict()
        table = get_table_cursor(collection)
        json_data = convert_str_to_obj_data(save_data, app_id)

        if return_document_bool:

            model_config = table.find_one_and_update(filter_data,
                                                     {method: json_data},
                                                     return_document=ReturnDocument.AFTER)
        else:
            model_config = table.find_one_and_update(filter_data,
                                                     {method: json_data})
        result['message'] = constant_utils.ENTITY_EDIT
        return model_config, result

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

'''
Updates multiple documents
'''
def update_many_collection_data(collection, filter_data, save_data, method, app_id=None):
    try:
        result = dict()
        table = get_table_cursor(collection)
        json_data = convert_str_to_obj_data(save_data, app_id)

        model_config = table.update_many(filter_data, json_data, upsert = True)
        result['message'] = constant_utils.ENTITY_EDIT
        return model_config, result

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

'''
Deletes record based on _id
'''
def delete_collection_data(_id, collection):
    """
    @summary:
    """
    try:
        table = get_table_cursor(collection)
        table.delete_one({"_id": ObjectId(_id)})
        json_data = constant_utils.ENTITY_DELETE
        return json_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


def delete_many_collection_data(collection, filter_data):
    """
    @summary:Deletes record based on filter data given
    """
    try:
        table = get_table_cursor(collection)
        table.delete_many(filter_data)
        json_data = constant_utils.ENTITY_DELETE
        return json_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


def get_document_count(collection, filter_data):
    try:
        table = get_table_cursor(collection)
        documents_count = table.find(filter_data).count()
        return documents_count

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


'''
fetch data according to field.
if field is _id, returns document dictionary, otherwise will retuen a list od dictionary.
'''
def fetch_user_collection_data(collection, app_id=None, field=None, parameter=None, key_to_be_removed=None, recent20=False):
    try:
        isID = False
        table = get_user_table_cursor(collection)
        user = get_table_data(table, app_id, field, parameter, recent20)

        if field == "_id":
            isID = True
        if user.count() == 0 and isID ==True:
            return json_util.dumps({})
        elif user.count() == 0 and isID ==False:
            return json_util.dumps([])
        converted_user_data = convert_obj_to_str_data(user, key_to_be_removed, isID) 
        return converted_user_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

'''
fetches collection documents based on multiple key  or complex queries
'''
def fetch_user_data_multiple_keys(collection, filter_data= {}, key_to_be_removed=None):
    try:
        isID = False
        table = get_user_table_cursor(collection)
        user = table.find(filter_data)
        converted_user_data = convert_obj_to_str_data(user, key_to_be_removed, isID)
        return converted_user_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


'''
Insert document to collection
'''
def insert_user_collection_data(collection, json_to_insert, app_id=None):
    try:
        json_dictionary = dict()
        table = get_user_table_cursor(collection)
        json_data = convert_str_to_obj_data(json_to_insert, app_id)
        user_details = table.insert_one(json_data)
        json_dictionary['_id'] = str(user_details.inserted_id)
        json_dictionary['message'] = constant_utils.ENTITY_CREATION

        return json_dictionary

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


'''
Update single document to collection and returns the updated document
                                if return_document_bool is set as true
'''
def update_user_collection_data(collection, filter_data, save_data, method, return_document_bool=False, app_id=None):
    try:
        result = dict()
        table = get_user_table_cursor(collection)
        json_data = convert_str_to_obj_data(save_data, app_id)

        if return_document_bool:

            model_config = table.find_one_and_update(filter_data,
                                                     {method: json_data},
                                                     return_document=ReturnDocument.AFTER)
        else:
            model_config = table.find_one_and_update(filter_data,
                                                     {method: json_data})
        result['message'] = constant_utils.ENTITY_EDIT
        return model_config, result

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

'''
Updates multiple documents
'''
def update_many_user_collection_data(collection, filter_data, save_data, method, app_id=None):
    try:
        result = dict()
        table = get_user_table_cursor(collection)
        json_data = convert_str_to_obj_data(save_data, app_id)

        model_config = table.update_many(filter_data, json_data, upsert = True)
        result['message'] = constant_utils.ENTITY_EDIT
        return model_config, result

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error

'''
Deletes record based on _id
'''
def delete_user_collection_data(_id, collection):
    """
    @summary:
    """
    try:
        table = get_user_table_cursor(collection)
        table.delete_one({"_id": ObjectId(_id)})
        json_data = constant_utils.ENTITY_DELETE
        return json_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error


def delete_many_user_collection_data(collection, filter_data):
    """
    @summary:Deletes record based on filter data given
    """
    try:
        table = get_user_table_cursor(collection)
        table.delete_many(filter_data)
        json_data = constant_utils.ENTITY_DELETE
        return json_data

    except ValueError as error:
        LOGGER.exception(error)
        raise error
    except Exception as error:
        LOGGER.exception(error)
        raise error