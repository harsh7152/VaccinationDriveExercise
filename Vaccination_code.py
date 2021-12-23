1. Read Employee data in sampleEmployee dataframe from here https://www.briandunning.com/sample-data/us-500.zip

2. Using this sampleEmployee dataframe create 100X more data points and store it into employeeDF ( this will have 50,000 rows )

3. The company has to plan a vaccination drive and the priority will be based on employee population in a particular city

4. Create a dataframe of CityEmployeeDensity, the 1st city will be the one with maxium number of employees

5. Please create new dataframe by name "VaccinationDrivePlan" with all columns from employeeDF and additional column "Sequence"

6. In Sequence Column populate the value from the CityEmployeeDensity Dataframe. Print this dataframe

7. Create a Final Vaccination Schedule with a plan to vaccinate 100 employees in a day

 Across Cities in Parallel

 OR

 Sequential

6. Print the final report showing in how many day the vaccination drive is completed per city

8. Once done put the code and output on your Github account and share with recruitment team


# Databricks notebook source
sampleEmployee  = spark.read.format('csv')\
                 .options(header='true')\
                 .load('dbfs:/XXXX/YYYY/ZZZ/us_500*')

# COMMAND ----------

from pyspark.sql.functions import expr
sampleEmployee_dup = sampleEmployee.withColumn('n', expr('explode(array_repeat(1,int(100)))'))

# COMMAND ----------
#population (number of emps) by city
import pyspark.sql.functions as f
CityEmployeeDensity = sampleEmployee_dup.groupby("city").count().select(f.col('city').alias('city_1') ,f.col('count').alias('no_of_emp')).orderBy(f.col("no_of_emp").desc())
CityEmployeeDensity.display()

# COMMAND ----------

from pyspark.sql.functions import col
VaccinationDrivePlan = CityEmployeeDensity.join(sampleEmployee_dup,CityEmployeeDensity.city_1 ==  sampleEmployee_dup.city, "inner").drop('city_1').withColumnRenamed('no_of_emp', 'Sequence').orderBy(f.col("Sequence").desc())

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import concat_ws
VDP = VaccinationDrivePlan.withColumn("Vaccine_Date", f.current_date())
VDP_1 = VDP.withColumn("ID", concat_ws('_',f.col("Sequence"),f.col("city")))
VDP_2 = VDP_1.orderBy(f.col("ID").desc())
VDP_3 = VDP_2.withColumn("Vaccine_Priority", f.floor(f.row_number().over(Window.orderBy(f.lit(None)))/100).cast('int'))
VDP_3.createOrReplaceTempView("Final")

# COMMAND ----------

#%sql
 Select *,date_add(Vaccine_Date,Vaccine_Priority)   from Final where city = 'Phoenix'

# COMMAND ----------

 %sql
 Select 
 City, 
 min(Vaccination_Date) as Vaccination_Start_Date, 
 max(Vaccination_Date) as Vaccination_End_Date, 
 datediff(max(Vaccination_Date),min(Vaccination_Date)) as No_of_days_for_vaccination  
 from 
 (
   Select a.city, date_add(Vaccine_Date,Vaccine_Priority) as Vaccination_Date, row_number() over(partition by a.city order by a.city desc) as rn from 
   (Select city, Sequence from Final group by city,sequence order by Sequence desc ) a
   left outer join final b
   on a.city = b.city
 )
 --where city = 'New Orleans'
 group by City
