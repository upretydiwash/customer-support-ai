from reader.bronze_table_reader import BronzeTableReader
from tests.data_quality_check import DataQualityPipeline
from trasformers.transformations import Transformations
import json
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils



#To get the DBUtils object
def get_dbutils(spark:SparkSession):
    try:
        return DBUtils(spark)
    
    except exception as e:
        print("Exception: ", e)

#creating a sparksession and returning the dbutils object which will have json file path 
def run_spark_session():
    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)
    return dbutils.widget.get("json_file_path")

#reading the json file 
def read_json_file(file):
    try:
        with open(file) as f:
            return json.load(f)
    except Exception as e:
        print("Exception: ", e)
        return None

if __name__ == "__main__":
    dbutils = run_spark_session()
    config = read_json_file(dbutils)
    workspace = config.get("workspace",'dev')
    catalog = config.get("catalog",'development')
    source_schema = config.get("source_schema",'bronze')
    source_table = congif.get("source_table")
    key_column = config.get("key_column")
    remove_dups_flag = config.get("remove_dups_flag",'N')
    remove_nulls_flag = config.get("remove_nulls_flag",'N')
    remove_nulls_column = config.get("remove_nulls_column",[])
    trim_columns_flag = config.get("trim_columns_flag",'N')
    trim_columns = config.get("trim_columns",[])
    quarantine_table = config.get("quarantine_table")
    rules = config.get("rules", {})
    #concating the source and the target tables with their proper names
    source_data_table = f'{catalog}.{source_schema}.{source_table}'
    target_data_table = f'{catalog}.{source_schema}.{quarantine_table}

    #now reading the source table from the config
    try:
        #read bronze table
        df = BronzeTableReader().read(source_data_table)
        #run tests on the extracted data
        try:
            
        


        





