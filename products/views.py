from django.shortcuts import render
# from rest_framework.response import Response
from django.http import HttpResponse
from rest_framework import status
from rest_framework.decorators import api_view,parser_classes
from . import models
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from django.utils.dateparse import parse_date
from _datetime import datetime
from bson.objectid import ObjectId

# Create your views here.
@api_view(["POST"])
@parser_classes((FormParser, MultiPartParser))
def create_product(request):
    try:
        json_to_insert = {
            "productName" : request.data.get('productName'),
            "productType" : request.data.get('productType'),
            "description" : request.data.get('description'),
            "stock" : int(request.data.get('stock')),
            "price" : int(request.data.get('price'))
        }
        
        result = models.create_product(json_to_insert)

        return Response(result,status=status.HTTP_200_OK)

    except Exception as error:
        return Response(error,status=status.HTTP_400_BAD_REQUEST)

@api_view(["GET"])
def get_all_products(request):
    try:
        response = models.get_all_products()

        return Response(response,status=status.HTTP_200_OK)
    except Exception as error:
        return Response(error,status=status.HTTP_400_BAD_REQUEST)


@api_view(["DELETE"])
@parser_classes((JSONParser,FormParser, MultiPartParser))
def delete_products(request):
    try:
        id = request.data.get("_id")
        result = models.delete_product(id)
        return Response(result,status=status.HTTP_200_OK)
    except Exception as error:
        return Response(error,status=status.HTTP_400_BAD_REQUEST)


@api_view(["PUT"])
@parser_classes((FormParser, MultiPartParser))
def update_product(request):
    try:
        id_to_update = request.data.get("_id")
        json_to_insert = {
            "productName" : request.data.get('productName'),
            "productType" : request.data.get('productType'),
            "description" : request.data.get('description'),
            # "images" : request.data.get('images'),
            "stock" : int(request.data.get('stock')),
            "price" : int(request.data.get('price'))
        }

        filter_data = {"_id":ObjectId(id_to_update)}
        result = models.update_product(filter_data,json_to_insert)
        return Response(result,status=status.HTTP_200_OK)

    except Exception as error:
        return Response(error,status=status.HTTP_400_BAD_REQUEST)


@api_view(["POST"])
@parser_classes((JSONParser, FormParser, MultiPartParser))
def get_product(request):
    try:
        id = request.data.get("_id")
        result = models.get_product(id)
        
        return Response(result,status=status.HTTP_200_OK)

    except Exception as error:
        return Response(error,status=status.HTTP_400_BAD_REQUEST)


@api_view(["POST"])
@parser_classes((JSONParser, FormParser, MultiPartParser))
def get_product_by_name(request):
    try:
        productName = request.data.get("productName")
        result = models.get_product_by_name(productName)
        
        return Response(result,status=status.HTTP_200_OK)

    except Exception as error:
        return Response(error,status=status.HTTP_400_BAD_REQUEST)


