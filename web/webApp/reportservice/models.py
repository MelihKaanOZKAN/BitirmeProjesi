import uuid
from cassandra.cqlengine import columns
from django_cassandra_engine.models import DjangoCassandraModel

class report(DjangoCassandraModel):
    reportId = columns.UUID(primary_key=True, default=uuid.uuid4)
    reportName = columns.Text()
    reportDate = columns.DateTime()
    sentimentId = columns.Integer()
    reportFilePath = columns.Text()

class tweet():
    tweetId = 0
    data = ""
    filteredWords = []
    sentiment = -1
    sentimentId = 0