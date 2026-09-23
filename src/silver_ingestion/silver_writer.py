class SilverTableWriter:

    def write(df, table_name):
        df.write.format('delta').mode("overwrite").saveAsTable(table_name)