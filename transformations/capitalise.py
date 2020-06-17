from pyspark.sql.functions import upper, col

def process(df, params=None, log=None):
    df_transformed = (
        df
        .select(
            col('id'),
            upper(col('first_name')).alias("first_name"),
            upper(col('second_name')).alias("second_name"),
            col('floor')))

    return df_transformed