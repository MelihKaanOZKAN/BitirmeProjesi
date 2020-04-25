import uuid
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
class tweet_bank(DjangoCassandraModel):
    tweetid = columns.Text(primary_key=True)
    pdt_sentiment = columns.Double()
    sentimentid = columns.Text()
    tweet = columns.Text()
    tweet_text = columns.Text()
    tweet_text_cleaned = columns.List(columns.Text)
