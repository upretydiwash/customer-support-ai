from pyspark.sql import functions as F

from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType
)


class Transformations:

    TYPE_MAP = {
        'int': IntegerType(),
        'integer': IntegerType(),
        'double': DoubleType(),
        'float': FloatType(),
        'boolean': BooleanType(),
        'bool': BooleanType(),
        'date': DateType(),
        'timestamp': TimestampType()
    }


    # Remove duplicates
    def remove_duplicates( df, primary_key):
        df = df.dropDuplicates([primary_key])
        return df
    
    # Remove nulls
    def remove_nulls( df, columns: list):
        return df.dropna(subset=columns)

    def trim_strings(df, columns: list):
        return df.withColumns({c: F.trim(F.col(c)) for c in columns})


    def normalize_ts( df, columns: list):
        return df.withColumns({c+'_date': F.col(c).cast(DateType()) for c in columns})
    

    def convert_to_type( df, column, type):
        return df.withColumn(column, F.col(column).cast(type))
    

    def capitalize(df, columns: list):
        return df.withColumns({c: F.initcap(F.col(c)) for c in columns})



