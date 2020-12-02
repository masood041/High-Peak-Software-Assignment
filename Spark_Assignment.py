from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)

df_marks = spark.read.option("header",True).csv("Marks.csv")
df_marks.createOrReplaceTempView('Marks')
# Query to Rank students by total marks
df1 = spark.sql(''' SELECT Rank() over (order by A.total_marks desc) AS RANK, A.studentId FROM (SELECT studentId, SUM(marks) AS total_marks FROM Marks GROUP BY studentId) A ''')
df1.toPandas().to_csv('Query1.csv',index=False)

# Query to find top three highest scorers for each subject
df2 = spark.sql(''' SELECT A.studentId,A.subject, A.rank FROM (SELECT Rank() over (PARTITION BY subject order by marks desc) as rank,studentId, subject FROM Marks) A WHERE A.rank<4 ''')
df2.toPandas().to_csv('Query2.csv',index=False)

# Find min ,max and mean scores for each subjects
df3 = spark.sql(''' SELECT subject, max(marks) as max, min(marks) as min, avg(marks) as mean FROM Marks GROUP BY subject ''')
df3.toPandas().to_csv('Query3.csv',index=False)
