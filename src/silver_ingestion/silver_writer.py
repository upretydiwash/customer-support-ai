class SilverTableWriter:

    def __init__(self,spark):
        self.spark = spark
    #overwite mode write
    def full_write(self,df, table_name):
        df.write.format('delta').mode("overwrite").saveAsTable(table_name)

    #incremental load write
    def incremental_write(self,tmp_view, target_table, merge_column):
        merge_column = "AND".join([f"s.{c} = t.{c}" for c in merge_column])
        merge_statement = f'''
                    MERGE INTO {target_table} t
                    USING {tmp_view} s
                    ON {merge_column}
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED
                    THEN INSERT *
                    '''
        merge_results_df = self.spark.sql(merge_statement)
        return merge_results_df


        