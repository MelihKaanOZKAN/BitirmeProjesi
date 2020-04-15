from django.shortcuts import render, HttpResponseRedirect, get_object_or_404
import datetime, uuid
from .forms import sentimentForm
from .models import *
from django.contrib import messages as msg
# Create your views here.
def sentiment_index(request):
    snts = sentiments.objects.all()
    context = {
        'sentiments' : snts
    }
    return render(request, 'sentimentManage/home.html', context)

def sentiment_detail(request, sentimentId):
    id = uuid.UUID(sentimentId)
    snt = get_object_or_404(sentiments, sentimentid = id)
    snt.check_none()
    context = {
        'sentiment': snt
    }
    return render(request, "sentimentManage/details.html", context=context)

def sentiment_start(request, sentimentId):
    id = uuid.UUID(sentimentId)
    snt = get_object_or_404(sentiments, sentimentid = id)
    snt.startSentiment()
    return HttpResponseRedirect(snt.get_absolute_url())

def sentiment_stop(request, sentimentId):
    id = uuid.UUID(sentimentId)
    snt: sentiments = get_object_or_404(sentiments, sentimentid = id)
    snt.stopSentiment()
    return HttpResponseRedirect(snt.get_absolute_url())
def sentiment_create(request):
   form = sentimentForm()
   if request.method == "POST":
       snt = sentiments()
       snt.sentimentname= request.POST.get("sentimentName")
       snt.mode= request.POST.get('mode')
       snt.keywords= request.POST.get('keywords').split(',')
       snt.notes= request.POST.get('notes').split(',')
       dt = datetime.datetime.now()
       snt.createdate =dt
       snt.lastupdate = dt
       f2 = snt.save()
       msg.success(request, "Success")
       return HttpResponseRedirect(f2.get_absolute_url())

   context =  {
       'form' : form
   }
   return render(request, "sentimentManage/startSentiment.html", context)

