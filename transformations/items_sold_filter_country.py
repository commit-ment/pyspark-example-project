from pyspark.sql.functions import col

def process(df, params=None, log=None):

    if not "value" in params:
        raise Exception("Transformation $items_sold_per_country$ expecting input argument with key $value$") 

    df_transformed = (
        df.where(col('Country') == params['value'])\
        .select(col('StockCode'), col('Total items'))\
        .orderBy(col("Total items").desc())
    )

    return df_transformed