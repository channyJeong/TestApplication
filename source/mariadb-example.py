# mariadb-example.py
from pyspark.sql import SparkSession

appName = "PySpark Example - MariaDB Example"
master = "spark://4f5a792df368:7077"
# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

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

#driver_df.show()

driver_df.createOrReplaceTempView("driver")
passenger_df.createOrReplaceTempView("passenger")

spark.sql("select * from driver").show()
spark.sql("select * from passenger").show()

spark.catalog.dropTempView("driver")
spark.catalog.dropTempView("passenger")



