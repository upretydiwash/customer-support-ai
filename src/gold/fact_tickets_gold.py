import sys
from pyspark.sql import SparkSession

#initiating spark session 
def run_spark_session():
    spark = SparkSession.builder.appName("fact_ticket_gold_job").getOrCreate()
    return spark

def create_tickets_gold(spark, environment):
    create_view_query = f'''
    CREATE TABLE IF NOT EXISTS {environment}.gold.fact_tickets_gold AS
SELECT 
t.ticket_id,
t.customer_id,
t.product_id,
t.created_at,
t.channel,
t.category_seed,
t.subcategory_seed,
t.subject,
t.message,
t.status,
t.priority_seed,
t.resolved_at,
t.ingested_ts as load_ts,
c.customer_name,
c.age,
c.province,
c.customer_segment,
c.customer_since,
p.product_name,
p.product_category,
case when t.resolved_at is null then 'N' else 'Y' end as is_resolved,
case when (lower(t.priority_seed) = 'high' or lower(t.priority_seed) = 'critical') 
and t.resolved_at is null and timestampdiff(HOUR, t.created_at, current_timestamp()) > 24 then 'Y' else 'N' end as is_priority_unresolved,
case when t.resolved_at is not null THEN timestampdiff(HOUR, t.created_at, t.resolved_at) else null end as resolution_time
FROM {environment}.silver.tickets_silver t
left join {environment}.silver.customer_silver c  
 on t.customer_id = c.customer_id
left join {environment}.silver.products_silver p
 on t.product_id = p.product_id

    '''
    spark.sql(create_view_query)
    return

def main():
    spark = run_spark_session()
    environment = sys.argv[1]
    if environment:
        print(f'Running in env: {environment}')
        create_tickets_gold(spark, environment)
    else:
        print('No environment provided, running in default')
        create_tickets_gold(spark, 'default')
    return
    
if __name__ == "__main__":
    main()