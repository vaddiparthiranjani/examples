from create_spark_object import spark

from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import  sumDistinct


df = spark.read.csv('C:\\Users\\dell\\PycharmProjects\\examples\\data.csv', header = True, inferSchema = True)
df.show()

# to get distinct count
#df.select(countDistinct('dept')).show()

# to get sum, avg, max, min sumDistinct
#df.select(sum('salary'),avg('salary'),max('salary'),min('salary'),sumDistinct('salary')).show()

#to sort salary ascending order
#df.sort(df.salary.asc()).show()

#to sort salary in descending order

#df.sort(df.salary.desc()).show()

#df.filter(df.salary >30000).show()
#df.filter(df.dept.contains('r')).show()
#df.filter(df.salary.between(20000, 40000)).show()
#df.filter(df.dept.like('%ng')).show()

from pyspark.sql.functions import when
#df.select(df.name, df.dept, df.salary, when(df.gender == 'm', 'Male') \
 #                                   .when(df.gender == 'f', 'Female').alias('new_gender') ).show()

# df.select(df.name).show()
#df.select(df['dept']).show()
#df1 = df.select(countDistinct('dept','gender'))
#df1.show()
