from pyspark.sql.functions import sum

def process(df, params=None, log=None):

    df_transformed = (
        df.groupBy('StockCode', 'Country').agg(sum("Quantity").alias("Total items")))

    return df_transformed