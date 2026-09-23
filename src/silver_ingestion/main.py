from reader.bronze_table_reader import BronzeTableReader
from tests.data_quality_check import DataQualityPipeline
from transformers.transformations import Transformations
from writer.silver_writer import SilverTableWriter
import json
from pyspark.sql import SparkSession, functions as F
import sys




#reading the json file 
def run_spark_session():
    spark = SparkSession.builder.appName("Spark Data Engineering").getOrCreate()
    return spark 

def read_json_file():
    try:
        file = sys.argv[1]
        print(f"Reading config file : {file}")
        with open(file) as f:
            return json.load(f)
    except Exception as e:
        print("Exception: ", e)
        return None

if __name__ == "__main__":
    spark = run_spark_session()
    config = read_json_file()
    workspace = config.get("workspace",'dev')
    catalog = config.get("catalog",'development')
    source_schema = config.get("source_schema",'bronze')
    source_table = config.get("source_table")
    key_column = config.get("key_column")
    remove_dups_flag = config.get("remove_dups_flag",'N')
    remove_nulls_flag = config.get("remove_nulls_flag",'N')
    remove_nulls_column = config.get("remove_nulls_column",[])
    trim_columns_flag = config.get("trim_columns_flag",'N')
    trim_columns = config.get("trim_columns",[])
    quarantine_table = config.get("quarantine_table")
    clean_table_schema = config.get("clean_table_schema")
    clean_table = config.get("clean_table")
    rules = config.get("rules", {})
    normalize_ts_flag = config.get("normalize_ts", 'N')
    normalize_ts_column = config.get("normalize_ts_column",[])
    convert_flag = config.get("convert_flag",'N')
    convert_columns = config.get("convert_columns",[])
    capitalize_flag = config.get("capitalize_flag",'N')
    capitalize_columns = config.get("capitalize_columns",[])
    
    #concating the source and the target tables with their proper names
    source_data_table = f'{catalog}.{source_schema}.{source_table}'
    target_quarantine_table = f'{catalog}.{source_schema}.{quarantine_table}'
    target_clean_table = f'{catalog}.{clean_table_schema}.{clean_table}'

    #now reading the source table from the config
    try:
        spark.sql(f'CREATE TABLE IF NOT EXISTS {target_clean_table}')
        #read bronze table
        bronze_reader = BronzeTableReader(spark)
        df = bronze_reader.read(source_data_table)
        #run tests on the extracted data
        try:
            print(f"Running data quality checks for table : {source_data_table}")
            DataCheck = DataQualityPipeline(df,target_quarantine_table,rules)
            df_clean, df_quarantine = DataCheck.data_quality()
            print(f'{df_clean.count()} passed the data checks!')
            print(f'{df_quarantine.count()} failed the data checks!')
        except Exception as e:
            raise Exception(f"Data quality checks failed for table : {source_data_table}: {e}") from e


        #transforming the data
        #removing dups
        try:
            if remove_dups_flag == 'Y':
                df_removed_dups = Transformations.remove_duplicates(df_clean,key_column)
                print(f'Removed {df_clean.count() - df_removed_dups.count()} duplicates! from {source_data_table}')
            else:
                df_removed_dups = df_clean

        except Exception as e:
            raise Exception(f"Data deduplication failed for table : {source_data_table} : {e}") from e 
        #removing nulls
        try:
            if remove_nulls_flag == 'Y' and remove_nulls_column:
                df_removed_nulls = Transformations.remove_nulls(df_removed_dups, remove_nulls_column)
                print(f'Removed {df_removed_dups.count() - df_removed_nulls.count()} nulls! from {source_data_table}')

            else:
                df_removed_nulls = df_removed_dups

        except Exception as e:
            raise Exception(f"Data null removal failed for table : {source_data_table} : {e}") from e

        #trimming columns
        try:
            if trim_columns_flag == 'Y' and trim_columns:
                df_trimmed = Transformations.trim_strings(df_removed_nulls, trim_columns)
                print(f"Trimmed columns for : {source_data_table}")

            else:
                df_trimmed = df_removed_nulls

        except Exception as e:
            raise Exception(f"Data trimming failed for table : {source_data_table} : {e}") from e

        #noramlizing ts
        try:
            if normalize_ts_flag == 'Y' and normalize_ts_column:
                df_normalized_ts = Transformations.normalize_ts(df_trimmed, normalize_ts_column)
                print(f"Normalized ts columns for : {source_data_table}")

            else:
                df_normalized_ts = df_trimmed

        except Exception as e:
            raise Exception(f"Data ts normalization failed for table : {source_data_table}: {e}") from e

        #converting columns
        try:
            if convert_flag == 'Y' and convert_columns:
                df_converted = Transformations.convert_to_type(df_normalized_ts, convert_columns)
                print(f"Converted columns for : {source_data_table}")

            else:
                df_converted = df_normalized_ts

        except Exception as e:
            raise Exception(f"Data type conversion failed for table : {source_data_table}: {e}") from e

        #capitalize columns
        try:
            if capitalize_flag == 'Y' and capitalize_columns:
                df_capitalized = Transformations.capitalize(df_converted, capitalize_columns)
                print(f"Capitalized columns for : {source_data_table}")

            else:
                df_capitalized = df_converted

        except Exception as e:
            raise Exception(f"Data capitalization failed for table : {source_data_table}: {e}") from e

        #writing the data to the clean table
        try:
            exception_column = ['_rescued_data','_ingested_at','_file','failed_rules','quarantined_at']
            df_capitalized = df_capitalized.drop(*exception_column).withColumn('ingested_ts', F.current_timestamp())
            SilverTableWriter.write(df_capitalized,target_clean_table)
            print(f"Data written to table : {target_clean_table}")

        except Exception as e:
            raise Exception(f"Data write failed for table : {target_clean_table}: {e}") from e

        print(f'Silver pipeline process completed for {target_clean_table}')
            
    except Exception as e:
        raise Exception(f"Data read failed for table : {source_data_table}: {e}") from e
            




            
        


        





