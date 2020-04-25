from django.shortcuts import render,  get_object_or_404, HttpResponseRedirect
from .forms import reportForm
from .models import *
import uuid, datetime
from django.contrib import messages as msg
# Create your views here.
import sys
sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/')
from utils.hdfsClient import client
def index(request, sentimentId):
    rpts = []
    try:
        rpts = report.objects.filter(sentimentid = sentimentId)

    except Exception as ex:
        print(str(ex))

    context = {
        'reports': rpts,
        'sentimentId' : sentimentId,
        "create": "/reportService/create/"+ sentimentId
    }
    return render(request, "reportService/home.html", context)
def createReport(request, sentimentId):
    form = reportForm()
    if request.method == "POST":
        rpt = report()
        rpt.sentimentid = sentimentId
        dt = datetime.datetime.now()
        rpt.reportdate = dt
        rpt.reportname = request.POST.get('reportname')
        rpt.reporttype = request.POST.get('reporttype')
        rpt = rpt.save()
        rpt.generateReport()
        msg.success(request, "Success")
        return HttpResponseRedirect(rpt.get_absolute_url())

    context = {
        'form': form
    }
    return render(request, "reportService/createReport.html", context)

def detail(request,reportid):
    rpt: report= get_object_or_404(report, reportid = reportid)
    c = client()
    if rpt.reporttype == "text":
        content = c.read(rpt.reportFilePath)
    elif rpt.reporttype == "pie":
        image = c.readByte(rpt.get_report_path())
        staticName = "/static/reports/report_{}.png".format(rpt.reportid)
        path = "/Users/melihozkan/Desktop/Projects/BitirmeProjesi/web/webApp" + staticName
        with open(path, "wb") as writer:
            writer.write(image)
        content = staticName

    context = {
        'report': rpt,
        'content': content
    }
    return render(request, "reportService/detail.html", context)
