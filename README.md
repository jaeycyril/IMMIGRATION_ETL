# IMMIGRATION_ETL

The main purpose of this prooject was to create a pipeline from scratch to process data from its raw
format after being extracted from the source systems to usable format from where useful insights can be derived.

Here, we used the immigration data for the United States to create a source of truth for further analysis.

The final source of truth can be used to answer questions regarding patterns of migration into and out of the US.
We can use the Date dimension to determine how many individuals came into the US for each month. 
This can help us see of there is any seasonality in the migration numbers or if there are peak months. 
This can help with better planning on the part of the migration office.

Using the visa dimension, we can see the most popular visa types granted to incoming migrants and find out the main reasons for their visit. 
We can also tell the avergae staying period for different visa types.

Also, the City dimension can show us the inflow of individuals into different cities in the US and over time, this can give us a picture of how much migration contributes towards the population in these cities.

Questions like these can help the migration department to plan how to deploy their resources to ensure smooth operations at the borders e.g. during peak periods. They can also help the Government with policy planning e.g. the number of people coming via a particular visa could be increased or reduced based on the needs of the Government at any particular time.

## STEP 1: SCOPE AND DATA GATHERING

In this project, we will combine immigration data with temperature data and US city demographics data to produce
a single source of truth for analysis. We will be using a Star schema to model the data to the required level
of granularity.

For this project, Spark via AWS EMR and S3 are the tools of choice because of their flexibily when performing ELT processes,
especially with datasets from very diverse sources. They also scale elastically, which means our solution is already future-proof
with respect to dataset size.

The datasets used are:
1. I94 Immigration data: This dataset contains immigration information for individuals coming into the United States. Details
include visa type, name, age, mode of arrival, arrival date, departure date etc. This dataset is from the US Govt office of Toursim and Trade.

2. World Temperature data: This dataset contains the average temperature of cities around the world for each month for the 1700s to 2013. 
This dataset is from Kaggle.

3. US Cities Demographics data: This dataset contains demographic data for cities in the US e.g. average household size, male population, female
population, race counts etc. this dataset is from OpenDataSoft.


## STEP 2: EXPLORE AND ASSESS DATA

Here, each of the datasets were reviewed to see a piece of their content. There suitability for the task ast hand was also judged.
The decision to create an outrigger dimension for race counts from the US Cities Demographics table and the decision to merge the country code 
to the Temperature data came at this point.

Also, the initial, manual cleaning of the dataset to create the input files for the ETL process was done here. Including checking null values counts
and removing duplicate rows.

See the Data_assess_and_cleaning.ipynb for more details.

Note, the output from this step were moved to S3, which was the source for the ETL process.


## STEP 3: DEFINE THE DATA MODEL

The data was modeled using the Star schema with one outrigger dimension for race counts. The star schema is a tried and tested 
method for modeling data. It was useful here because it allowed us bring together all the tables at a level of granularity which 
we could determine. During analysis, the fact table can be sliced and diced along each of the dimensions we have.

![Data model diagram](/immigration_data_model.drawio.png)

Data Pipeline steps:
1. Place each dataset in the S3 buckets after the initial manual assessment
2. Process the categorical data in the I94 data dictionary and place the resulting datasets in s3 as well.
3. Process the US Cities Demographics data to create the Dimension table of the same name
4. Process the Race Counts data to create the Race Counts outrigger dimension table
5. Create the Temperature dimension table by extracting the country codes from the I94 dataset and joining to the World Temperature dataset
6. Create the junk dimesnion of Visa type and Mode of Entry by doing a cross join of both datasets
7. Process the I94 dataset to create the Date Dimension table
8. Process the I94 dataset to create the Immigration Fact table
9. Save all the resulting tables in S3.


## STEP 4: RUN THE ETL
The pipeline was run in Spark using AWS EMR and the main data quality checks included:
- Type checking for the columns
- Volume checking for th final tables

See ETL.py for more details.

## STEP 5

The Fact table containing Immigration data would need to be updated daily because the migration is a phenomenon that occurs daily. A scheduled batch process
at the end of the day can be used to make the updates.

**If the dataset was increased by 100x**, we could tune the parameters of EMR and ot would work still. S3 is also capable of handling the consequent size of the
data so this would not cause issues. Elasticity and scalability is an area where cloud platforms shine.

**If the data populates a dashboard daily**, then it would be effectuve to wrap up that update process into Airflow. Airflow allows us to schedule tasks like these while giving us fine grained control over the timing. It also allows us connect easily to EMR to run the process when necessary.

**If the data needs to be accessed by 100+**, The final tables are in parquet format, so we could load them into Redshift (or other large scale production ready Data Warehouse solutions) which allows multiple concurrent connections.


## SAMPLE QUERY

Suppose we wanted to see the distribution of visa types for incoming travellers, we could use the following code:

```sql
SELECT v.Visa_type, count(*) as Immigrant_count
FROM immigration_fact a
JOIN Visa_mode_dimension v
ON a.i94visa = v.Visa_code
GROUP BY 1
```

![Query result](/result.png)

We see that most people are coming in for tourism first, then for business, before study.
