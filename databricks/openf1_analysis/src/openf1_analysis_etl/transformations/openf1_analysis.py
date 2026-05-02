from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark import pipelines as dp


@dp.table(
    name="openf1.bronze.circuit",
    table_properties={
        "quality": "bronze",
    },
)
def circuit():

    return spark.read.table("openf1_db.openf1.circuit")


@dp.table(
        name="openf1.bronze.country",
        table_properties={
            "quality": "bronze"
        },
)
def country():
    return spark.read.table("openf1_db.openf1.country")


@dp.table(
    name='openf1.bronze.drivers',
    table_properties = {
        "quality": "bronze"
    }
)
def driver():

    return spark.read.table("openf1_db.openf1.drivers")


@dp.table(
    name="openf1.bronze.meeting",
    table_properties = {
        "quality": "bronze"
    }

)
def meetings():

    return spark.read.table("openf1_db.openf1.meeting")

@dp.table(
    name="openf1.bronze.sessions",
    table_properties = {
        "quality": "bronze"
    }

)
def sessions():

    return spark.read.table("openf1_db.openf1.sessions")

