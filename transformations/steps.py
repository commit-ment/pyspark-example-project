from pyspark.sql.functions import col, lit

def process(df, params=None, log=None):

    if not "steps_per_floor" in params:
        raise Exception("Transformation $steps$ expecting input argument with key $steps_per_floor$") 

    df_transformed = (
        df
        .select(
            col('id'),
            col('first_name'),
            col('second_name'),
            col('floor'),
            (col('floor')*lit(params["steps_per_floor"])).alias("steps")))

    return df_transformed