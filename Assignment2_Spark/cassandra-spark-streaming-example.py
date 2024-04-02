from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,LongType,IntegerType,FloatType,StringType, TimestampType
from pyspark.sql.functions import split,from_json,col

songSchema = StructType([
    StructField("id", IntegerType(), False),
    StructField("person", StringType(), False),
    StructField("time", TimestampType(), False),
    StructField("song", StringType(), False)
])

csvSchema = StructType([
    StructField("name", StringType(), True),
    StructField("artists", StringType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("album_name", StringType(), True),
    StructField("album_release_date", StringType(), True),
    StructField("danceability", FloatType(), True),
    StructField("energy", FloatType(), True),
    StructField("key", IntegerType(), True),
    StructField("loudness", FloatType(), True),
    StructField("mode", IntegerType(), True),
    StructField("speechiness", FloatType(), True),
    StructField("acousticness", FloatType(), True),
    StructField("instrumentalness", FloatType(), True),
    StructField("liveness", FloatType(), True),
    StructField("valence", FloatType(), True),
    StructField("tempo", FloatType(), True)
])

spark = SparkSession.builder.appName("SSKafka").config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:29092").option("subscribe", "songs_stream").option("startingOffsets", "latest").load() 

sdf = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), songSchema).alias("data")).select("data.*")

#read CSV data
csv_df = spark.read.csv("spotify-songs.csv", header=True, schema=csvSchema).cache()

joined_df = sdf.join(csv_df, sdf.song == csv_df.name, "left_outer")            #Left join on the name of the song

           

#Print schema of the DataFrame
print("DataFrame Schema:")
sdf.printSchema()



# Start the streaming query
query = joined_df.writeStream.format("console").start()
print("Streaming query started...")



def writeToCassandra(writeDF, _):
  print("Writing to Cassandra...")
  writeDF.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="records", keyspace="spotify").save()
  print("Write to Cassandra successful!")

result = None
while result is None:
  try:
    result = joined_df.writeStream \
    .option("spark.cassandra.connection.host", "localhost:9042") \
    .foreachBatch(writeToCassandra) \
    .outputMode("update") \
    .trigger(processingTime = '30 seconds').start().awaitTermination()
  except:
    pass
