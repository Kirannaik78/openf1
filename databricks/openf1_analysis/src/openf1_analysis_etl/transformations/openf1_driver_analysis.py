from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="openf1.sliver.drivers_details",
    table_properties = {
        "quality": "sliver"
    }
)
def drivers_datails():

    drivers = spark.read.table("openf1.bronze.drivers")
    sessions = spark.read.table("openf1.bronze.sessions")

    total_sessions_by_racers = sessions.join(F.broadcast(drivers), on='session_key', how='inner').filter(F.col('session_type')=='Race').groupBy(F.col('full_name')).agg(F.count(F.col('session_key')).alias('total_session')).select(F.col("full_name"), F.col("total_session"))

    return total_sessions_by_racers