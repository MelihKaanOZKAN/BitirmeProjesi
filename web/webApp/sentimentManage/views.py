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
    snt.isSparkContextRunning()
    context = {
        'sentiment': snt
    }
    return render(request, "sentimentManage/details.html", context=context)

def sentiment_start(request, sentimentId):
    id = uuid.UUID(sentimentId)
    snt = get_object_or_404(sentiments, sentimentid = id)
    if snt.status != "Running":
        if  snt.startSentiment():
            msg.success(request, sentimentId + " Started")
        else:
            msg.error(request, "Process  exists")
    else:
        msg.error(request, sentimentId +" already running.")

    return HttpResponseRedirect(snt.get_absolute_url())

def sentiment_log(request, sentimentId):
    id = uuid.UUID(sentimentId)
    snt : sentiments= get_object_or_404(sentiments, sentimentid = id)
    path = snt.get_log_path()
    if snt.startdate == None:
        msg.error(request, "No Log. Sentiment not running.")
        return HttpResponseRedirect(snt.get_absolute_url())
    import sys
    sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/')
    from utils.hdfsClient import client
    cl = client()
    log = cl.read(path)
    context ={
        'sentimentLog' : log,
        'sentimentId': sentimentId
    }

    return render(request, "sentimentManage/log.html", context)

def sentiment_stop(request, sentimentId):
    id = uuid.UUID(sentimentId)
    snt: sentiments = get_object_or_404(sentiments, sentimentid = id)
    if snt.status == "Running":
        if snt.stopSentiment():
            msg.success(request, sentimentId + " Stop Signal Sended")
        else:
            msg.error(request, "Process is not running")
    elif snt.status == "DEAD":
        msg.error(request, sentimentId + " is dead")
    else:
        msg.error(request, sentimentId + " stop signal already sended. Wait for process tweets")
    return HttpResponseRedirect(snt.get_absolute_url())
def sentiment_create(request):
   form = sentimentForm()
   if request.method == "POST":
       snt = sentiments()
       snt.sentimentname= request.POST.get("sentimentName")
       snt.mode= request.POST.get('mode')
       snt.keywords= request.POST.get('keywords').split(',')
       snt.notes= request.POST.get('notes').split(',')
       snt.method = request.POST.get('method_')
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

