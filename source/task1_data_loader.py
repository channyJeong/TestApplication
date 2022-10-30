# project.py
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import col,lit,split

appName = "Data Engineer - Test Appliaction"
master = "spark://4f5a792df368:7077"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()


# Kafka Producer (Local File To Kafka Broker)
#c_date = str(datetime.now().date())
#df = spark.read.text("/spark/project_data/booking.csv").withColumn("key", lit(c_date))
#df.selectExpr("key","value").write.format("kafka").option("kafka.bootstrap.servers", "172.22.0.6:9092").option("topic", "booking").save()

# Kafka Consumer (Kafka Broker To Application - booking Topic)
kd = spark.read.format("kafka").option("kafka.bootstrap.servers", "172.22.0.6:9092").option("subscribe", "booking").load()
temp_df = kd.selectExpr("CAST(value AS STRING)")
booking_df = temp_df.select(split(col("value"), ",").getItem(0).alias("id"), split(col("value"), ",").getItem(1).alias("date_create"), split(col("value"), ",").getItem(2).alias("id_driver"), split(col("value"), ",").getItem(3).alias("id_passenger"), split(col("value"), ",").getItem(4).alias("rating"), split(col("value"), ",").getItem(5).alias("start_date"), split(col("value"), ",").getItem(6).alias("end_date"), split(col("value"), ",").getItem(7).alias("tour_value"))


# MariaDB Data Load (driver, passenger table)
driver_sql = "select * from project.driver"
passenger_sql = "select * from project.passenger"
user = "project"
password = "1234"
jdbc_url = "jdbc:mysql://172.22.0.4:3306/project?permitMysqlScheme"
jdbc_driver = "org.mariadb.jdbc.Driver"

# Create a data frame by reading data from Oracle via JDBC
driver_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", driver_sql) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", jdbc_driver) \
    .load()

passenger_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", passenger_sql) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", jdbc_driver) \
    .load()


# Create TempView
driver_df.createOrReplaceTempView("driver")
passenger_df.createOrReplaceTempView("passenger")
booking_df.createOrReplaceTempView("booking")

# Data Processing
spark.sql("select * from driver").show()
spark.sql("select * from passenger").show()
spark.sql("select * from booking").show()

# Drop TempView
spark.catalog.dropTempView("driver")
spark.catalog.dropTempView("passenger")
spark.catalog.dropTempView("booking")



