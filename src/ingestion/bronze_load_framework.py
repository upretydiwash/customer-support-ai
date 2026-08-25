from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name



class BronzeAutoLoaderFramework:
  def __init__(self, table_metadata:dict):
      self.spark = SparkSession.builder.getOrCreate()
      self.workspace = table_metadata.get('workspace')
      self.catalog = table_metadata.get('catalog')
      self.table = table_metadata.get('table')
      self.volume = table_metadata.get('volume')
      self.schema = table_metadata.get('schema')
      self.infer_schema = table_metadata.get('infer_schema')
      self.source_format = table_metadata.get('source_format')
      self.csv_options = table_metadata.get('cvs_options',{})
      
      self.checkpoint = f'{self.volume}_checkpoints/'
      self.schema_path = f'{self.volume}_schemas/'
      self.source = f'{self.catalog}.{self.schema}.{self.table}'


  def run_ingetion(self):
      print("Executing Bronze Auto Loader")
      print(f'For table {self.table}')
      
      try:
        reader = (
        self.spark.readStream.format('cloudFiles')
        .option('cloudFiles.format', self.source_format))
        .option('cloudFiles.schemaLocation', self.schema_path)

        if self.source_format.lower() == 'csv':
            reader = reader.option('cloudFiles.inferColumnTypes', self.infer_schema)
            for key,value in self.csv_options.items():
                reader = reader.option(key,value)
        
        raw_df = reader.load(self.volume)

        bronze_df = raw_df.withColumn('_ingested_at', current_timestamp())\
            .withColumn('_file', input_file_name())  


      query = (
          bronze_df.writeStream
          .format('delta')
          .option('checkpointLocation', self.checkpoint)
          .trigger(once=True
      )
        