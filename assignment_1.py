from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# entrypoint
spark = SparkSession.builder.enableHiveSupport().appName('ETl job').getOrCreate()
spark.sql('set spark.sql.sources.partitionOverwriteMode=dynamic')

# readoing csv  file
reading = spark.read.csv('sample.csv', sep="|", header="True")

# drop columns not requierd
df = reading.drop('_c0', 'H')

# casting new schema
df2 = df.withColumn('Customer_Name', col('Customer_Name').cast(StringType())) \
    .withColumn('Customer_Id', col('Customer_Id').cast(StringType())) \
    .withColumn('Open_Date', to_date(col('Open_Date'), "yyyymmdd").cast(DateType())) \
    .withColumn('Last_Consulted_Date', to_date(col('Last_Consulted_Date'), "yyyymmdd").cast(DateType())) \
    .withColumn('Vaccination_Id', col('Vaccination_Id').cast(StringType())) \
    .withColumn('Dr_Name', col('Dr_Name').cast(StringType())) \
    .withColumn('State', col('State').cast(StringType())) \
    .withColumn('Country', col('Country').cast(StringType())) \
    .withColumn('DOB', to_date(col('DOB'), "ddmmyyyy").cast(DateType())) \
    .withColumn('Is_Active', col('Is_Active').cast(StringType()))

# users input for table name previous or new
name = input("enter your table name :----->")

try:

    if not name:
        name = 'default_table'
        df2.write.option("header", "true"). \
            saveAsTable(name=name, format='csv', mode='overwrite', partitionBy='Country')

except:
    print("table not found")

# showing currunet path of file saved
cwd = os.getcwd() + '/spark-warehouse/' + name + '/'
print("file has been saved in ", cwd)

# now reading data from tables
country_options = df2.select('Country').distinct().show()

try:
    which_country = input('which country data do you want ----->')
    reading_data = spark.read.csv(path='file:' + cwd + 'Country=' + which_country.upper(),
                                  sep=",", header="True", inferSchema='True')
    reading_data.printSchema()
    reading_data.show()

except:
    print('plz enter correct country or path not found')

