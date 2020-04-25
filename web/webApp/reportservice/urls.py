from django.conf.urls import url
from .views import *
app_name = 'report'
urlpatterns = [
    #url('', sentiment_index),
    url(r'index/(?P<sentimentId>[0-9a-f-]+)/', index),
    url(r'detail/(?P<reportid>[0-9a-f-]+)/', detail),
    url(r'create/(?P<sentimentId>[0-9a-f-]+)/', createReport),
]
