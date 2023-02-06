import pandas as pd
data = [["Ran","100"],["kiran","200"],["Ram","120"],["Pavan","900"]]

pandasDF = pd.DataFrame(data , columns = ['Name','Id'])

print(pandasDF)


from pyspark.sql.types import StructType, StructField, StringType
from create_spark_object import spark


#sparkDF=spark.createDataFrame(pandasDF)
#print(sparkDF.printSchema())
#print(sparkDF)

mySchema = StructType([StructField("Name", StringType(), True),
                      StructField("Id", StringType(), True)])

sparkDF = spark.createDataFrame(pandasDF, schema=mySchema)

sparkDF.printSchema()
sparkDF.show()
