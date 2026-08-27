class BronzeTableReader:

    def read(self, table_name):
        return self.spark.read.table(table_name) 


    