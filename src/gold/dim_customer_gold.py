import sys
from pyspark.sql import SparkSession

#initiating spark session 
def run_spark_session():
    spark = SparkSession.builder.appName("dim_customer_gold_job").getOrCreate()
    return spark

def create_cusomer_gold(spark, environment):
    create_view_query = f'''
    CREATE TABLE IF NOT EXISTS {environment}.gold.dim_customer_gold AS
    SELECT
        customer_id as customer_key,
        customer_name,
        age,
        CASE WHEN age >=18 and age <=25 THEN '18-25'
              WHEN age >=26 and age <=35 THEN '26-35'
              WHEN age >=36 and age <=50 THEN '36-50'
              WHEN age >=501 THEN '51+'
              ELSE 'Unknown'
        END AS age_group,
        province,
        customer_segment,
        customer_since,
        ROUND(DATEDIFF(current_date(), customer_since)/365,1) AS tenure_years,
        ingested_ts as load_ts
        from {environment}.silver.customer_silver
    '''
    spark.sql(create_view_query)
    return

def main():
    spark = run_spark_session()
    environment = sys.argv[1]
    if environment:
        print(f'Running in env: {environment}')
        create_cusomer_gold(spark, environment)
    else:
        print('No environment provided, running in default')
        create_cusomer_gold(spark, 'default')
    return
    
if __name__ == "__main__":
    main()