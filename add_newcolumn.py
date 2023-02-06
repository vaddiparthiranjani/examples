from create_spark_object import spark
from pyspark.sql.functions import lit, current_date, when

df = spark.read.csv('C:\\Users\\dell\\PycharmProjects\\examples\\data.csv', header= True, inferSchema= True)

df.show()
if 'salary1' not in df.columns:
    print("aa")
df.withColumn("bonus percent", lit(0.3))

df.withColumn("bonus amount",df.salary * 0.3)

df.withColumn("date", current_date()).show()

df.withColumn("grade", \
              when((df.salary <20000),lit('A')) \
                .when((df.salary >=20000) & (df.salary <50000), lit('B')) \
                .otherwise(lit('C')) \
              ).show()

