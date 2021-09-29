import pandas as pd

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

import datetime
import os, sys
import configparser

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    
    return spark


def process_city_demographics(spark, input_data, output_data):
    city_demographics = spark.read.csv(f"{input_data}/city_demographics_cleaned.csv", header=True)
    
    city_demographics = city_demographics.withColumn("Median Age", F.col("Median Age").cast(T.DoubleType()))\
                                    .withColumn("Male Population", F.col("Male Population").cast(T.IntegerType()))\
                                    .withColumn("Female Population", F.col("Female Population").cast(T.IntegerType()))\
                                    .withColumn("Total Population", F.col("Total Population").cast(T.IntegerType()))\
                                    .withColumn("Number of Veterans", F.col("Number of Veterans").cast(T.IntegerType()))\
                                    .withColumn("Foreign-born", F.col("Foreign-born").cast(T.IntegerType()))\
                                    .withColumn("Average Household Size", F.col("Average Household Size")\
                                                 .cast(T.DoubleType()))
    
    city_demographics = city_demographics.withColumnRenamed("Median Age", "Median_Age")\
                 .withColumnRenamed("Male Population", "Male_Population")\
                 .withColumnRenamed("Female Population", "Female_Population")\
                 .withColumnRenamed("Total Population", "Total_Population")\
                 .withColumnRenamed("Number of Veterans", "Number_of_Veterans")\
                 .withColumnRenamed("Average Household Size", "Average_Household_Size")\
                 .withColumnRenamed("State Code", "State_Code")
    
    city_demographics = city_demographics.na.fill(0, ["Male_Population", "Female_Population", "Total_Population", 
                                                  "Number_of_Veterans", "Average_Household_Size"])
    
    # Quality check
    if city_demographics.count() < 1:
        raise Exception("City Demographics dataset does not contain any data")
    
    city_demographics.write.mode("overwrite").parquet(f"{output_data}/city_demographics_dim.parquet")
    

    
def process_race_counts_dimension(spark, input_data, output_data):
    
    race_counts = spark.read.csv(f"{input_data}/race_counts.csv", header=True)
    race_counts = race_counts.withColumn("Count", F.col("Count").cast(T.IntegerType()))
    race_counts = race_counts.withColumnRenamed("Count", "Race_Population")
    
    race_counts.write.mode("overwrite").parquet(f"{output_data}/race_counts_dim.parquet")
    

    
def process_temp_dimension(spark, input_data, output_data):
    temp_data = spark.read.csv(f"{input_data}/GlobalTemperature_clean.csv", header=True)
    
    temp_data = temp_data.withColumn("dt", F.col("dt").cast(T.DateType()))\
                  .withColumn("AverageTemperature", F.col("AverageTemperature").cast(T.DoubleType()))\
                  .withColumn("AverageTemperatureUncertainty", F.col("AverageTemperatureUncertainty")\
                              .cast(T.DoubleType()))
    
    temp_data = temp_data.withColumn("temp_year", F.year(F.col('dt')))\
            .withColumn("temp_month", F.month(F.col('dt')))
    
    i94_cc = spark.read.csv(f"{input_data}/I94COUNTRY_output.csv", header=True, inferSchema=True)
    
    i94_cc = i94_cc.withColumn("Country_for_join", F.lower(F.col('Country')))
    
    i94_cc = i94_cc.withColumn("Country_for_join", F.trim(F.col('Country_for_join')))
    
    country_renaming = {'china, prc':'china',
              'ivory coast': "côte d'ivoire",
              'mexico air sea, and not reported (i-94, no land arrivals)':'mexico',
              'bosnia-herzegovina': 'bosnia and herzegovina',
              'guinea-bissau': 'guinea bissau',
               'congo': 'congo (democratic republic of the)'}
    
    for old_country, new_country in country_renaming.items():
        i94_cc = i94_cc.withColumn("Country_for_join", F.when(F.col('Country_for_join')== old_country, new_country)\
                  .otherwise(F.col('Country_for_join')))
    
    i94_cc_for_join = i94_cc.select("Code", "Country_for_join")
    
    temp_code_data = temp_data.join(i94_cc_for_join, temp_data.Country==i94_cc_for_join.Country_for_join, "left")\
                     .drop("Country_for_join")
    
    temp_code_data = temp_code_data.withColumn("Country", F.initcap(F.col('Country')))
    
    temp_code_data = temp_code_data.dropDuplicates()
    
    # quality check
    if temp_code_data.count() < 1:
        raise Exception("Temperature Dimension has no data")
    
    temp_code_data.write.mode("overwrite").parquet(f"{output_data}/temp_dim.parquet")
    
    

def process_visa_mode_dimension(spark, input_data, output_data):
    visa_data = spark.read.csv(f"{input_data}/I94VISA.csv", inferSchema=True, header=True)
    mode_data = spark.read.csv(f"{input_data}/I94MODE_output.csv", inferSchema=True, header=True)
    
    visa_data = visa_data.withColumnRenamed("Code", "Visa_Code")
    mode_data = mode_data.withColumnRenamed("Code", "Mode_Code")
    
    visa_and_mode_dim = visa_data.crossJoin(mode_data)
    
    # quality check 1
    if visa_and_mode_dim.count() < 1:
        raise Exception("Visa and Mode junk Dimension has no data")


    # quality check 2: Check that no extra categories exist
    if visa_and_mode_dim.select(F.countDistinct('Visa_type')).collect()[0][0] != 3:
        raise Exception("Visa type should have exactly 3 distinct values.")

    if visa_and_mode_dim.select(F.countDistinct('Mode')).collect()[0][0] != 4:
        raise Exception("Mode type should have exactly 4 distinct values.")
    
    visa_and_mode_dim.write.mode("overwrite").parquet("{output_data}/visa_and_mode_dim.parquet")
    

    
def process_time_dim(spark, input_data, output_data):
        
    immig_data = spark.read.parquet("{input_data}/sas_data")
        
    immig_data = immig_data.withColumn("arrival_date", F.expr("date_add('1960-01-01', arrdate)"))\
          .withColumn("departure_date", F.expr("date_add('1960-01-01', depdate)"))
        
    df_arr_dates = immig_data.select("arrival_date")
        
    df_dep_dates = immig_data.select("departure_date")
        
    df_dates = df_arr_dates.union(df_dep_dates)
        
    df_dates = df_dates.dropDuplicates()
        
    df_dates = df_dates.withColumnRenamed("arrival_date", "migration_date")
        
    df_dates = df_dates.withColumn("Year", F.year("migration_date"))\
                   .withColumn("Month", F.month("migration_date"))\
                   .withColumn("DayOfMonth", F.dayofmonth("migration_date"))\
                   .withColumn("DayOfWeek", F.dayofweek("migration_date"))\
                   .withColumn("DayOfYear", F.dayofyear("migration_date"))\
                   .withColumn("WeekofYear", F.weekofyear("migration_date"))\
                   .withColumn("Quarter", F.quarter("migration_date"))\
    
    # quality check
    if df_dates.count() < 1:
        raise Exception("Date Dimension has no data")
        
    df_dates.write.mode("overwrite").parquet("{output_data}/Date_dim.parquet")


def process_immigration_fact(spark, input_data, output_data):
    
    immig_data = spark.read.parquet("sas_data")
    
    for col, coltype in immig_data.dtypes:
        if coltype == 'double':
            immig_data = immig_data.withColumn(col, F.col(col).cast(T.IntegerType()))
    
    immig_data = immig_data.withColumn("arrival_date", F.expr("date_add('1960-01-01', arrdate)"))\
          .withColumn("departure_date", F.expr("date_add('1960-01-01', depdate)"))
    
    immig_data = immig_data.dropDuplicates()
    
    # quality check
    if immig_date.count() < 1:
        raise Exception("Fact table has no data")
    
    immig_data.write.mode('overwrite').parquet('{output_data}/migration_fact.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://datalake-etl-cleaned_input/"
    output_data = "s3a://datalake-etl-output"
    
    process_city_demographics(spark, input_data, output_data)
    process_race_counts_dimension(spark, input_data, output_data)
    process_temp_dimension(spark, input_data, output_data)
    process_visa_mode_dimension(spark, input_data, output_data)
    process_time_dim(spark, input_data, output_data)
    process_immigration_fact(spark, input_data, output_data)
    

if __name__ == "__main__":
    main()