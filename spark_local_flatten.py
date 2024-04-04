from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Create Spark session
spark = SparkSession \
	.builder \
	.master("local") \
	.appName("ClickStreamRead") \
	.getOrCreate()

# Read json data into dataframe
df = spark.read.options(inferSchema = 'True', delimiter = ',') \
	.json('/user/ec2-user/cab_ride_analysis/kafka/clickstreamdump/*.json')

# Extract columns from json value in dataframe and create new dataframe with new columns
df = df.select( \
	get_json_object(df["value_str"], "$.customer_id").alias("cusomter_id"), \
	get_json_object(df["value_str"], "$.app_version").alias("app_version"), \
	get_json_object(df["value_str"], "$.OS_version").alias("OS_version"), \
	get_json_object(df["value_str"], "$.lat").alias("lat"), \
	get_json_object(df["value_str"], "$.lon").alias("lon"), \
	get_json_object(df["value_str"], "$.page_id").alias("page_id"), \
	get_json_object(df["value_str"], "$.button_id").alias("button_id"), \
	get_json_object(df["value_str"], "$.is_button_click").alias("is_button_click"), \
	get_json_object(df["value_str"], "$.is_page_view").alias("is_page_view"), \
	get_json_object(df["value_str"], "$.is_scroll_up").alias("is_scroll_up"), \
	get_json_object(df["value_str"], "$.is_scroll_down").alias("is_scroll_down"), \
	get_json_object(df["value_str"], "$.timestamp").alias("timestamp"), \
	)

# convertBoolean = udf(lambda x: 1 if x == 'Yes' else 0)

# booleanList = ['is_button_click', 'is_page_view', 'is_scroll_up', 'is_scroll_down']

# for i in booleanList:
# 	df = df.withColumn(i, convertBoolean(col(i)))

# Print schema of dataframe with new columns
print(df.schema)

# Print 10 records from dataframe
df.show(10)

# Save dataframe to csv file with headers in first row in local file directory
df.coalesce(1).write.format('com.databricks.spark.csv').save('/user/ec2-user/cab_ride_analysis/kafka/clickstreamdump/csv', header = 'true')

