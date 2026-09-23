class BronzeTableReader:

    def __init__(self, spark):
        self.spark = spark

    def read(self, table_name):
        return self.spark.read.table(table_name) 


    