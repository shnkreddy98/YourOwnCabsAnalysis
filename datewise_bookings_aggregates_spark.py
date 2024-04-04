from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session
spark = SparkSession \
	.builder \
	.master("local") \
	.appName("AggreagateData") \
	.getOrCreate()

# Read data into dataframe with schema
df = spark.read.options(inferSchema = 'True', delimiter = ',') \
	.csv('/user/ec2-user/cab_ride_analysis/sqoop/bookings/part-m-00000')

# Adding headers to the bookings data
df_withCol = df.toDF("booking_id", "customer_id", "driver_id", \
	"customer_app_version", "customer_phone_os_version", "pickup_lat", \
	"pickup_lon", "drop_lat", "drop_lon", "pickup_timestamp", \
	"drop_timestamp", "trip_fare", "tip_amount", "currency_code", \
	"cab_color", "cab_registration_no", "customer_rating_by_driver", \
	"rating_by_customer", "passenger_count")

df_withCol.show(10)

# Converting Timestamp to data format
df_withCol = df_withCol.withColumn('Date', df_withCol['pickup_timestamp'].cast('date'))

print(df_withCol.schema)

# Grouping on Date column to get the bookings count per day
aggDF = df_withCol.groupBy("Date").count().withColumnRenamed("count","Bookings_Count")
aggDF.show(10)

# Saving dataframe into csv format directly onto the hdfs
aggDF.repartition(1).write.format('com.databricks.spark.csv').save('/user/ec2-user/cab_ride_analysis/aggBookings/results', header = 'true')

