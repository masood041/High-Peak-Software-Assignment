from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
sc = SparkContext.getOrCreate()        
sql_sc = SQLContext(sc)

df_marks = sql_sc.read.csv("Marks.csv")
df_marks.createOrReplaceTempView('Marks')
# Query to Rank students by total marks
df1 = sql_sc.sql(''' SELECT Rank() over (order by A.total_marks desc) AS RANK, A.studentId FROM (SELECT studentId, SUM(marks) AS total_marks FROM Marks GROUP BY studentId) A ''')

# Query to find top three highest scorers for each subject
df2 = sql_sc.sql(''' SELECT A.studentId, A.subject, A.rank FROM (SELECT studentId, subject, Rank() over subject (order by marks desc) as rank FROM Marks) A WHERE A.rank <=3 ''')

# Find min ,max and mean scores for each subjects
df3 = sql_sc.sql(''' SELECT subject, max(marks) as max, min(marks) as min, avg(marks) as mean FROM Marks GROUP BY subject ''')
