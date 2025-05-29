from pyspark.sql import SparkSession 
spark = SparkSession.builder \
    .master("spark://172.17.0.2:7077") \
    .enableHiveSupport() \
    .getOrCreate()

df_results=spark.read.option('header', True).csv("hdfs://172.17.0.2:9000/buckets/results.csv")
df_constructors=spark.read.option('header',True).csv("hdfs://172.17.0.2:9000/buckets/constructors.csv") 
df_driver=spark.read.option('header', True).csv("hdfs://172.17.0.2:9000/buckets/drivers.csv") 
df_race=spark.read.option('header', True).csv("hdfs://172.17.0.2:9000/buckets/races.csv")

df_driver=df_driver.select("driverId", "forename","surname","nationality")
df_results=df_results.select("resultId", "raceId", "constructorId", "driverId", "points") 

df_data=df_driver.join(df_results, df_driver["driverId"]==df_results["driverId"], "inner").select("forename", "surname", "nationality", "points") 

df_data=df_data.select(df_data["forename"], df_data["surname"], df_data["nationality"], df_data["points"].cast("int"))

df_data.write.insertInto("formula1.driver_results")

df_data_constructor=df_constructors.join(df_results, df_constructors["constructorId"]==df_results["constructorId"], "inner").select("constructorRef", "name", "nationality", "url", "points") 

df_data_2=df_data_constructor.select(df_data_constructor["constructorRef"], df_data_constructor["name"], df_data_constructor["nationality"], df_data_constructor["url"], df_data_constructor["points"].cast("int"))

df_data_2.write.insertInto("formula1.constructor_results")