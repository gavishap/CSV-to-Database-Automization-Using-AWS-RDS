#!/usr/bin/env python
# coding: utf-8

# # The Task At Hand

# In this Jupyter notebook I will be completing the City-Hive exercise that has been handed to me. This project at the time of completion will be able to:
# 
# 1)Upload a dataset from a website containing corona statistics from a CSV format into a AWS database of PostgreSQL format.
# 
# 2)Run a query that answers the following question and export its result into a csv file: How many countries does the dataset include?
# 
# 3)Upload that csv file into an AWS S3 bucket.

# # Plan of Action

# *Step 1*
# 
#     A) Import all the packages needed for this project
#     B) Get the URL of the hyperlink that needs to clicked in order to download the dataset
#     C) Download the dataset and save it to our directory
#     D) Create an SQL statement to give to our AWS databse to create our table
#     E) Connect to our AWS database
#     F) Creat an empty table with the names of the columns in our database
#     G) Copy the rest of the data into the database
# 
# *Step 2*
# 
#     H) Make a pandas query function to give the results of the unique countries in the location column.
#     I) Save and export the results of the query into our working directory
#     
# *Step 3*
# 
#     J) Connect to the S3 bucket I made
#     K) Upload the CSV file to the bucket

import re
import urllib.request
import boto3
import pandas as pd
import psycopg2
import os

#getting the URL of the hyperlink to download the CSV file
html = urllib.request.urlopen("https://ourworldindata.org/covid-deaths")
text = html.read()
plaintext = text.decode('utf8')
links = re.findall("href=[\"\'](.*?)[\"\']", plaintext)
csv_link = [link for link in links if "csv" in link]
print(csv_link[0])



#Downloading and saving the dataset from the link
from urllib.request import urlretrieve as retrieve

retrieve(csv_link[0], 'CoronaStats.csv')


# Create a SQL statement to create our database table so that we can import the csv we downloaded
# To do so we will get the names of the columns and then join the column name with the correct data type it needs when defining a table.
# After that add a comma between each line. Now because the data type names are different in pandas than SQL, we shall make a dictionary to replace each
# column data type in pandas to the correct type name in SQL.



#use pandas to get names of coloumns of the csv
ipynb_path = os.path.dirname(os.path.realpath("__file__"))
csv_file_path = ipynb_path + '\\CoronaStats.csv' 
csv_data = pd.read_csv(csv_file_path)
column_names = list(csv_data.columns.values)
print(column_names)


#type replacement dictionary
replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
}

col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(column_names, csv_data.dtypes.replace(replacements)))



# Connect to our AWS database
conn_string = "host=coronastatistics.cz9th7gv5riq.us-east-1.rds.amazonaws.com                 dbname=''                user='postgres' password='Password'"
try:
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print("opened database succesfully")
except:
    print("Unable to connect to database")


# Creat an empty table with the names of the columns in our database

#create table without data
full_sql_query = "create table coronadatastats" + '(' + col_str + ')'
try:
    cursor.execute(full_sql_query)
except:
    print("Table already exists, lets continue")
    pass



#copying the data from the csv file to the database
try:
    my_file = open(csv_file_path)
except:
    print("Could not open csv file")
SQL_STATEMENT = """
COPY coronadatastats FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
 """
try:
    cursor.copy_expert(sql = SQL_STATEMENT, file = my_file)
    print("file copied to db")
except:
    print("Could not copy data into database")




try:
    cursor.execute("grant select on table coronadatastats to public")
except:
    print("Already made it public")
    pass
conn.commit()
cursor.close()
print("table coronadatastats imported to db completed")


#Make a pandas query function to give the results of the unique countries in the location column.


#run a pandas query on the data to find unique countries number
country_count = csv_data['location'].nunique()
print(country_count)


# Save and export the results of the query into our working directory

#create pandas table that holds the distinct country count
distinct_countries = {'count' : [country_count]}
distinct_countries_df = pd.DataFrame(distinct_countries)
print (distinct_countries_df)


#export it to where our code is running
with open("count_countries.csv", "w") as count_countries:
    distinct_countries_df.to_csv(count_countries, index=False)

#made a special user that has access to the s3 bucket
access_key='AKIATXLDSL5MH2CGKUHE'
secret_access_key='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'

#connect to the s3 bucket
client = boto3.client('s3', aws_access_key_id = access_key , aws_secret_access_key = secret_access_key)

#search through directory and find the CSV file we saved and upload it to s3
for file in os.listdir():
    if 'count_countries.csv' in file:
        upload_file_bucket = 'city-hive-exercise-bucket'
        upload_file_key = 'coronastats' + str(file)
        client.upload_file(file, upload_file_bucket, upload_file_key)
        break
print("CSV file uploaded to Bucket succesfully")
        
        

