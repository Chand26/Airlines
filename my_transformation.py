import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "stage_bookings",
    comment="Raw bookings data from bronze layer, acting as a streaming source for the silver pipeline."
)

def stage_bookings():
    return spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronze_volume/bookings/")

@dlt.view(
    name = "trans_bookings",
    comment = "Applies cleaning and transformation logic to bookings data."
)

def trans_bookings():
    return (
        dlt.read_stream("stage_bookings").withColumn("amount", col("amount").cast(DecimalType(10,2))).withColumn("booking_date", to_date(col("booking_date"), "yyyy-MM-dd")).withColumn("modified_date", current_timestamp()).drop("_rescued_data")
    )

rules = {
    "valid_booking_id": "booking_id IS NOT NULL",
    "valid_passenger_id": "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings",
    comment = "The final, cleaned bookings table with data quality constraints.")

@dlt.expect_all_or_drop(rules)

def silver_bookings():
    return dlt.read_stream("trans_bookings")

@dlt.view(
    name = "trans_flights"
)

def trans_flights():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronze_volume/flights/")
    df = df.withColumn("modified_date", current_timestamp()).drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_flights")
dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)

@dlt.view(
    name = "trans_passengers"
)

def trans_passengers():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronze_volume/passengers/")
    df = df.withColumn("modified_date", current_timestamp()).drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)

@dlt.view(
    name = "trans_airports"
)

def trans_airports():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronze_volume/airports/")
    df = df.withColumn("modified_date", current_timestamp()).drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1    
)

@dlt.table(name = "silver_business")

def silver_business():
    return (
        dlt.read_stream("silver_bookings").join(dlt.read_stream("silver_flights"),"flight_id").join(dlt.read_stream("silver_passengers"),"passenger_id").join(dlt.read_stream("silver_airports"),"airport_id").drop("modified_date")
    )