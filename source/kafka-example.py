from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col,lit,split


appName = "PySpark Example - Kafka Producer/Consumer Example"
master = "spark://4f5a792df368:7077"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()


#producer
#c_date = str(datetime.now().date())
#df = spark.read.text("/spark/project_data/booking.csv").withColumn("key", lit(c_date))
#df.selectExpr("key","value").write.format("kafka").option("kafka.bootstrap.servers", "172.22.0.6:9092").option("topic", "booking").save()

#print("producing complete")

#consume
kd = spark.read.format("kafka").option("kafka.bootstrap.servers", "172.22.0.6:9092").option("subscribe", "booking").load()
temp_data = kd.selectExpr("CAST(value AS STRING)")
print("consume complete")

data = temp_data.select(split(col("value"), ",").getItem(0).alias("id"), split(col("value"), ",").getItem(1).alias("date_create"), split(col("value"), ",").getItem(2).alias("id_driver"), split(col("value"), ",").getItem(3).alias("id_passenger"), split(col("value"), ",").getItem(4).alias("rating"), split(col("value"), ",").getItem(5).alias("start_date"), split(col("value"), ",").getItem(6).alias("end_date"), split(col("value"), ",").getItem(7).alias("tour_value"))

data.createOrReplaceTempView("booking") 

spark.sql("select * from booking").show()

spark.catalog.dropTempView("booking")



