import sys
from pyspark.sql import SparkSession

#initiating spark session 
def run_spark_session():
    spark = SparkSession.builder.appName("fact_ticket_events_gold_job").getOrCreate()
    return spark

def create_ticket_events_gold(spark, environment):
    create_view_query = f'''
    CREATE TABLE IF NOT EXISTS {environment}.gold.fact_ticket_events_gold AS
SELECT 
te.event_id,
te.ticket_id,
te.agent_id,
te.event_type,
te.event_timestamp,
c.customer_id,
t.product_id,
t.channel,
t.category_seed,
t.priority_seed,
t.status,
t.created_at,
t.resolved_at,
ROW_NUMBER() OVER(PARTITION BY te.ticket_id ORDER BY te.event_timestamp) as event_sequence,
case when event_sequence = 1 then 'TRUE' else 'FALSE' end as is_first_event,
case when lower(t.status) ='resolved' then 'TRUE' else 'FALSE' end as is_resolved_event,
coalesce(DATEDIFF(MINUTE,t.created_at,t.resolved_at),NULL) as resolution_time,
te.ingested_ts as load_ts
FROM {environment}.silver.ticket_events_silver te
LEFT OUTER JOIN {environment}.silver.tickets_silver t
ON te.ticket_id = t.ticket_id
LEFT OUTER JOIN {environment}.silver.customer_silver c
ON t.customer_id = c.customer_id

    '''
    spark.sql(create_view_query)
    return

def main():
    spark = run_spark_session()
    environment = sys.argv[1]
    if environment:
        print(f'Running in env: {environment}')
        create_ticket_events_gold(spark, environment)
    else:
        print('No environment provided, running in default')
        create_ticket_events_gold(spark, 'development')
    return
    
if __name__ == "__main__":
    main()