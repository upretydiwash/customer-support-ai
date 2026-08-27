class SilverTableWriter:

    def write(self, df, table_name):
        df.write.format('delta').mode("overwrite").saveAsTable(table_name)