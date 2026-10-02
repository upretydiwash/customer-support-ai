import sys
from pyspark.sql import SparkSession

#initiating spark session 
def run_spark_session():
    spark = SparkSession.builder.appName("dim_products_gold_job").getOrCreate()
    return spark

def create_cusomer_gold(spark, environment):
    create_view_query = f'''
    CREATE TABLE IF NOT EXISTS {environment}.gold.dim_products_gold AS
SELECT
  product_id AS product_key,
  product_name,
  product_category,
  CASE
    WHEN product_category IN ('Credit Card', 'Line of Credit') THEN 'Credit'
    WHEN product_category IN ('Loan', 'Mortgage') THEN 'Lending'
    WHEN product_category IN ('Banking', 'Business Banking') THEN 'Banking'
    WHEN product_category IN ('Payments', 'Digital') THEN 'Digital & Payments'
    WHEN product_category = 'Investments' THEN 'Investments'
    ELSE 'Other'
  END AS product_division,
  ingested_ts AS load_ts
FROM {environment}.silver.products_silver
WHERE product_id IS NOT NULL
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