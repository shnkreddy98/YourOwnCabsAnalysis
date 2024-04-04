from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession \
	.builder \
	.master("local") \
	.appName("ClickStreamRead") \
	.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

kafka_servers = "18.211.252.152:9092"
topic = "de-capstone3"

# Create dataframe from kafka data
df = spark \
	.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", kafka_servers) \
	.option("subscribe", topic) \
	.option("startingOffsets", "earliest")  \
	.load()

# Transform dataframe by dropping few columns and changing value column data types
df = df  \
	.withColumn('value_str', df['value'].cast('string').alias('key_str')).drop('value') \
	.drop('key', 'topic', 'partition', 'offset', 'timestamp', 'timestampType')

# Write the dataframe to local file directory and keep it running until termination
df.writeStream \
	.outputMode("append") \
	.format("json") \
	.option("path", "/user/ec2-user/cab_ride_analysis/kafka/clickstreamdump") \
	.option("checkpointLocation", "/user/ec2-user/cab_ride_analysis/kafka/checkpoint") \
	.start() \
	.awaitTermination()	

