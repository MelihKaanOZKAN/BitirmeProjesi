from apache_spark.store.cassandra.sqlFunctions import SqlFunctions
class save():
    def __init__(self):
        self.Sql = SqlFunctions()
    def save(self,df,spark):
        self.Sql.savePredicts_DF(df,spark)