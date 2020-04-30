import uuid, pdfkit
from cassandra.cqlengine import columns
from django_cassandra_engine.models import DjangoCassandraModel
import sys
sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/')
from utils.hdfsClient import client
import matplotlib.pyplot as plt
class report(DjangoCassandraModel):
    reportid = columns.UUID(primary_key=True, default=uuid.uuid4)
    reportname = columns.Text()
    reportdate = columns.DateTime()
    sentimentid = columns.Text()
    reportFilePath = columns.Text()
    reporttype = columns.Text()

    def get_absolute_url(self):
        return "/reportService/detail/{}".format(self.reportid)
    def get_report_path(self):
        return "/reports/report_{}.report".format(self.reportid)
    def get_pdf_download(self):
        self.generatePdf()
        return self.getStatic()
    def __get__results(self):
        total = tweet_bank.objects.filter(sentimentid=self.sentimentid).count()
        processed = tweet_bank.objects.filter(sentimentid=self.sentimentid, pdt_sentiment__gte=0).count()
        positive = tweet_bank.objects.filter(sentimentid=self.sentimentid, pdt_sentiment=1).count()
        negative = tweet_bank.objects.filter(sentimentid=self.sentimentid, pdt_sentiment=0).count()
        return total, processed, positive, negative

    def get_text_result(self):
        total, processed, positive, negative = self.__get__results()
        c = client()
        result = "Total: {} \n Processed = {} \n Positive = {} - {} % \n Negative: {} - {} %".format(total, processed, positive, round(float(positive * 100/processed), 1), negative, round(float(negative * 100/processed), 1))
        path = self.get_report_path()
        c.overwrite(path, result)
        self.reportFilePath = path
        self.save()
    def get_pie_chart(self):
        total, processed, positive, negative = self.__get__results()
        unprocessed = total - processed
        c = client()
        labels = ["Unprocessed", "Positive", "Negative"]
        sizes = [unprocessed, positive, negative]
        explode = (0, 0.1, 0.1)
        fig1, ax1 = plt.subplots()
        ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%',
                shadow=True, startangle=90)
        ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        path = self.get_report_path()
        c.writeMathplot(path, plt)
        self.reportFilePath = path
        self.save()
    def generateReport(self):
        if self.reporttype == "text":
            self.get_text_result()
        elif self.reporttype == "pie":
            self.get_pie_chart()
    def writeToHTMLFromTemplate(self):
        html = ""
        with open('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/web/webApp/templates/reportService/report.html', "r") as read:
            html += read.read()
        html = html.replace('[Report_Id]', str(self.reportid))
        html = html.replace('[Report_Name]', self.reportname)
        date = str(self.reportdate.day) + "/" + str(self.reportdate.month) + "/" + str(self.reportdate.year)  + " " + str(self.reportdate.hour) +"."+str(self.reportdate.minute) + ":" + str(self.reportdate.second)
        html = html.replace('[Report_Date]', date)
        html = html.replace('[SentimentId]', self.sentimentid)
        html = html.replace('[content]', self.readhdfs())
        import sys, uuid
        sys.path.append('/Users/melihozkan/Desktop/Projects/BitirmeProjesi/web/webApp')
        from sentimentManage.models import sentiments

        snt:sentiments = sentiments.objects.get(sentimentid = uuid.UUID(self.sentimentid))
        date = str(snt.startdate.day) + "/" + str(snt.startdate.month) + "/" + str(
            snt.startdate.year) + " " + str(snt.startdate.hour) + "." + str(
            snt.startdate.minute) + ":" + str(snt.startdate.second)

        html = html.replace('[startDate]',date)
        date = str(snt.stopdate.day) + "/" + str(snt.stopdate.month) + "/" + str(
            snt.stopdate.year) + " " + str(snt.stopdate.hour) + "." + str(
            snt.stopdate.minute) + ":" + str(snt.stopdate.second)
        html = html.replace('[stopDate]', date)
        html = html.replace('[method]', snt.method)
        html = html.replace('[sentimentName]', snt.sentimentname)
        keywords = ""
        for index, i in enumerate(snt.keywords):
            keywords+=i
            if index < len(snt.keywords) -1:
                keywords += ", "
        notes = ""
        for index, i in enumerate(snt.notes):
            notes += i
            if index < len(snt.notes) - 1:
                notes += ", "
        html = html.replace('[keywords]', keywords)
        html = html.replace('[notes]', notes)
        import datetime
        now = datetime.datetime.now()
        date = str(now.day) + "/" + str(now.month) + "/" + str(now.year)
        html = html.replace('[now]', date)
        return html
    def readhdfs(self):
        c = client()
        if self.reporttype == "text":
            read = c.read(self.reportFilePath)
            return read.replace('\n','<br>')
        elif self.reporttype == "pie":
            staticName = "/static/reports/report_{}.png".format(self.reportid)
            path = "/Users/melihozkan/Desktop/Projects/BitirmeProjesi/web/webApp" + staticName
            content = "<img style=\"width:500px; height=500px\" src=\"{}\" alt=\"Pie Report\">".format(path)
            return content
    def getStatic(self):
        return "/static/reports/report_{}.pdf".format(self.reportid)
    def generatePdf(self):
        html = self.writeToHTMLFromTemplate()
        pdfkit.from_string(html, "/Users/melihozkan/Desktop/Projects/BitirmeProjesi/web/webApp" + self.getStatic())

class tweet_bank(DjangoCassandraModel):
    tweetid = columns.Text(primary_key=True)
    pdt_sentiment = columns.Double()
    sentimentid = columns.Text()
    tweet = columns.Text()
    tweet_text = columns.Text()
    tweet_text_cleaned = columns.List(columns.Text)
