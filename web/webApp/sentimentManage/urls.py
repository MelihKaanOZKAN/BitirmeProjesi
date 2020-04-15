from django.conf.urls import url
from .views import *
app_name = 'home'
urlpatterns = [
    #url('', sentiment_index),
    url('index/', sentiment_index, name='home'),
    url(r'detail/(?P<sentimentId>[0-9a-f-]+)/', sentiment_detail),
    url(r'start/(?P<sentimentId>[0-9a-f-]+)/', sentiment_start),
    url(r'stop/(?P<sentimentId>[0-9a-f-]+)/', sentiment_stop),
    url('index/', sentiment_index),
    url('startsentiment/', sentiment_create),
]
