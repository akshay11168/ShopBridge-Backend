from django.conf.urls import url, include
from rest_framework import serializers
from django.urls import path

# from rest_framework.urlpatterns import format_suffix_patterns

from . import views

urlpatterns = [
    url(r'^create/$', views.create_product),
    url(r'^get/$',views.get_all_products),
    url(r'^delete/$',views.delete_products),
    url(r'^update/$',views.update_product),
    url(r'^getproduct/$',views.get_product),
    url(r'^getproductByName/$',views.get_product_by_name),
]
