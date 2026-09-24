import sys
from bronze_table_reader import BronzeTableReader
from silver_writer import SilverTableWriter
from data_quality_check import DataQualityPipeline
from transformations import Transformations
from pyspark.sql import functions as F, SparkSession
import json


#run spark session
def run_spark_session():
    spark = SparkSession.builder.appName("customer_silver").getOrCreate()
    return spark

# read config file 
def read_json_file():
    try:
        file = sys.argv[1]
        print(f'Reading config: {file}')
        with open(file) as f:
            config = json.load(f)
        return config
    except Exception as e:
        print(f'Error reading config: {e}')
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
    silver_writer_mode = config.get("silver_writer_mode")
    merge_columns = config.get("merge_columns")
    #concating source and targets with proper name 
    source_data_table =  f'{catalog}.{source_schema}.{source_table}'
    target_quarantine_table = f'{catalog}.{source_schema}.{quarantine_table}'
    target_clean_table = f'{catalog}.{clean_table_schema}.{clean_table}'
    

    #reading source table:
    try:
        bronze_reader = BronzeTableReader(spark)
        df = bronze_reader.read(source_data_table)
        max_date_query = f'SELECT max(ingested_ts) from {target_clean_table}' 
        last_proc_date = spark.sql(max_date_query)
        raw_date = last_proc_date.collect()[0][0]
        last_proc_date_value = raw_date if raw_date is not None else "1900-01-01"
        try:
            incremental_df = df.filter(df._ingested_at > last_proc_date_value)
            if incremental_df.count() > 0:
                #running data quality checks for incremental load
                try:
                    DataCheck = DataQualityPipeline(incremental_df, target_quarantine_table, rules)
                    df_clean, df_quarantine = DataCheck.data_quality()
                    print(f'{df_clean.count()} passed the data checks.')
                    print(f'{df_quarantine.count()} failed the data checks.')
                except Exception as e:
                    raise Exception(f'Error running data quality checks: {e}')
                #transformation layer
                #removing dups
                try:
                    if remove_dups_flag == 'Y':
                        df_removed_dups = Transformations.remove_duplicates(df_clean, key_column)
                        print(f'Removed {df_clean.count() - df_removed_dups.count()} dupliucations from {source_data_table}')

                    else:
                        df_removed_dups = df_clean
                except Exception as e:
                    raise Exception(f'Error removing duplicates: {e}')
                
                #remove nulls
                try:
                    if remove_nulls_flag == 'Y':
                        df_removed_nulls = Transformations.remove_nulls(df_removed_dups, remove_nulls_column)
                        print(f'Removed {df_removed_dups.count() - df_removed_nulls.count()} nulls from {source_data_table}')

                    else:
                        df_removed_nulls = df_removed_dups
                except Exception as e:
                    raise Exception(f'Error removing nulls: {e}')
                
                #trimming columns
                try:
                    if trim_columns_flag == 'Y' and trim_columns:
                        df_trimmed = Transformations.trim_strings(df_removed_nulls, trim_columns)
                        print(f'Trimmed columns for : {source_data_table}')
                    
                    else:
                        df_trimmed = df_removed_nulls
                except Exception as e:
                    raise Exception(f'Error trimming columns: {e}')
                                    
                #normalizing timestamp
                try:
                    if normalize_ts_flag == 'Y' and normalize_ts_column:
                        df_normalized_ts = Transformations.normalize_ts(df_trimmed, normalize_ts_column)
                        print(f'Normalized timestamp for : {source_data_table}')
                    else:
                        df_normalized_ts = df_trimmed
                except Exception as e:
                    raise Exception(f'Error normalizing timestamp: {e}')
                
                #converting columns

                try:
                    if convert_flag == 'Y' and convert_columns:
                        df_converted = Transformations.convert_to_type(df_normalized_ts, convert_columns)
                        print(f'Converted columns for : {source_data_table}')
                    else:
                        df_converted = df_normalized_ts        
                except Exception as e:
                    raise Exception(f'Error converting columns: {e}')

                #capitalize columns
                try:
                    if capitalize_flag == 'Y' and capitalize_columns:
                        df_capitalized = Transformations.capitalize(df_converted, capitalize_columns)
                        print(f'Capitalized columns for : {source_data_table}')
                    else:
                        df_capitalized = df_converted
                except Exception as e:
                    raise Exception(f'Error capitalizing columns: {e}')
                
                #writing to the silver table
                try:
                    exception_column =  ['_rescued_data','_ingested_at','_file','failed_rules','quarantined_at']
                    df_capitalized = df_capitalized.drop(*exception_column).withColumn('ingested_ts', F.current_timestamp())
                    df_capitalized.createOrReplaceTempView('temp_bronze_data')
                    """
                    merge_statement = f'''
                    MERGE INTO {target_clean_table} t
                    USING temp_bronze_data s
                    ON t.customer_id = s.customer_id
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED
                    THEN INSERT *
                    '''
                    merge_results_df = spark.sql(merge_statement)
                    """
                    merge_results_df = SilverTableWriter(spark).incremental_write('temp_bronze_data', target_clean_table, merge_columns)
                    metrics = merge_results_df.collect()[0]

                    print('MERGE statement executed successfully!')
                    print(f'Rows inserted: {getattr(metrics, 'num_affected_rows', 'N/A')}')
                    print(f'Rows updated: {getattr(metrics, 'num_updated_rows', 'N/A')} ')

                except Exception as e:
                    raise Exception(f'Error writing to silver table: {e}')
                    
                finally:
                    spark.catalog.dropTempView('temp_bronze_data')
            
            else:
                print(f'No new data found for {source_data_table}')   
                
        except Exception as e:
            raise Exception(f'Error processing {source_data_table}: {e}')
    
    except Exception as e:
        raise Exception(f'Error processing {source_data_table}: {e}')
                    

 
                    
                    
                
             
                                    

                        

                        
                
                

            

        
            

    










