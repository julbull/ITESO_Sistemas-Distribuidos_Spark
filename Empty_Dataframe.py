# %%
import pyspark
import pandas

# Create a spark session
spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
 
# Create an empty RDD
emp_RDD = spark.sparkContext.emptyRDD()

data_schema =   [
                StructField("YEAR",                 IntegerType(),      True),
                StructField("MONTH",                IntegerType(),      True),
                StructField("DAY",                  IntegerType(),      True),
                StructField("DAY_OF_WEEK",          IntegerType(),      True),
                StructField("AIRLINE",              StringType(),       True),
                StructField("FLIGHT_NUMBER",        IntegerType(),      True),
                StructField("TAIL_NUMBER",          StringType(),       True),
                StructField("ORIGIN_AIRPORT",       StringType(),       True),
                StructField("DESTINATION_AIRPORT",  StringType(),       True),
                StructField("SCHEDULED_DEPARTURE",  IntegerType(),      True),
                StructField("DEPARTURE_TIME",       IntegerType(),      True),
                StructField("DEPARTURE_DELAY",      IntegerType(),      True),
                StructField("TAXI_OUT",             IntegerType(),      True),
                StructField("WHEELS_OFF",           IntegerType(),      True),
                StructField("SCHEDULED_TIME",       IntegerType(),      True),
                StructField("ELAPSED_TIME",         IntegerType(),      True),
                StructField("AIR_TIME",             IntegerType(),      True),
                StructField("DISTANCE",             IntegerType(),      True),
                StructField("WHEELS_ON",            IntegerType(),      True),
                StructField("TAXI_IN",              IntegerType(),      True),
                StructField("SCHEDULED_ARRIVAL",    IntegerType(),      True),
                StructField("ARRIVAL_TIME",         IntegerType(),      True),
                StructField("ARRIVAL_DELAY",        IntegerType(),      True),
                StructField("DIVERTED",             IntegerType(),      True),
                StructField("CANCELLED",            IntegerType(),      True),
                StructField("CANCELLATION_REASON",  IntegerType(),      True),
                StructField("AIR_SYSTEM_DELAY",     IntegerType(),      True),
                StructField("SECURITY_DELAY",       IntegerType(),      True),
                StructField("AIRLINE_DELAY",        IntegerType(),      True),
                StructField("LATE_AIRCRAFT_DELAY",  IntegerType(),      True),
                StructField("WEATHER_DELAY",        IntegerType(),      True)
                ]
final_struc = StructType(fields=data_schema)


# Create an empty RDD with empty schema
data = spark.createDataFrame(data = emp_RDD,
                             schema = final_struc)
 
# Print the dataframe
print('Dataframe :')
data.show()
 
# Print the schema
print('Schema :')
data.printSchema()

# Save Empty DataFrame
data.write.csv("empty_dataframe/output")



