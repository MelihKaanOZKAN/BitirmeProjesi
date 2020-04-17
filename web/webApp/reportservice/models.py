import uuid
from cassandra.cqlengine import columns
from django_cassandra_engine.models import DjangoCassandraModel

class report(DjangoCassandraModel):
    reportId = columns.UUID(primary_key=True, default=uuid.uuid4)
    reportName = columns.Text()
    reportDate = columns.DateTime()
    sentimentId = columns.Integer()
    reportFilePath = columns.Text()

class tweet_bank(DjangoCassandraModel):
    tweetid = columns.Text(primary_key=True)
    pdt_sentiment = columns.Double()
    sentimentid = columns.Text()
    tweet = columns.Text()
    tweet_text = columns.Text()
    tweet_text_cleaned = columns.List(columns.Text)
