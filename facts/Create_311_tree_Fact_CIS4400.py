#!/usr/bin/env python
# coding: utf-8

# In[1]:


# ETL Complaint Facts
# If using the native Google BigQuery API module:
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
# import credentials
import pandas as pd
import os
import pyarrow
from datetime import datetime
from google.oauth2 import service_account


# In[2]:


df = pd.DataFrame
# Set the name of the dimension
fact_name = 'requests'

# Set the GCP Project, dataset and table name
gcp_project = 'cis-4400-406318'
bq_dataset = '311_newtree_dataset'
table_name = fact_name + '_fact'
# Construct the full BigQuery path to the table
fact_table_path = ".".join([gcp_project,bq_dataset,table_name])

# Set the path to the source data files
# For Linux use something like    /home/username/python_etl
# For Mac use something like     /users/username/python_etl
# file_source_path = 'c:\\Python_ETL'
file_source_path = '/Users/liudmilakitaeva/311_tree_requests.csv'
path_to_service_account_key_file = '/Users/liudmilakitaeva/Downloads/cis-4400-406318-6261595b1732.json'


# In[3]:


def transform_data( df):
    """
    transform_data
    Accepts a data frame
    Performs any specific cleaning and transformation steps on the dataframe
    Returns the modified dataframe
    """
    # Convert the date to a datetime64 data type. 2012-08-21 04:12:16.827
    df['created_date'] = pd.to_datetime(df['created_date'], format="%Y-%m-%d %H:%M:%S.%f")
    df['closed_date'] = pd.to_datetime(df['closed_date'], format="%Y-%m-%d %H:%M:%S.%f")
    # Convert the postal code into a string
    df['incident_zip'] =  df['incident_zip'].astype(str)
    return df


# In[4]:


def upload_bigquery_table(bqclient, table_path, write_disposition, df):
    """
    upload_bigquery_table
    Accepts a path to a BigQuery table, the write disposition and a dataframe
    Loads the data into the BigQuery table from the dataframe.
    for credentials.
    The write disposition is either
    write_disposition="WRITE_TRUNCATE"  Erase the target data and load all new data.
    write_disposition="WRITE_APPEND"    Append to the existing table
    """
    try:
        
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
        
        # Submit the job
        job = bqclient.load_table_from_dataframe(df, table_path, job_config=job_config)
        
        # Show the job results
        job.result()
    except Exception as err:
        print("Failed to load BigQuery Table.", err)
        # os._exit(-1)


# In[5]:


def bigquery_table_exists(table_path, bqclient):
    """
    bigquery_table_exists
    Accepts a path to a BigQuery table
    Checks if the BigQuery table exists.
    Returns True or False
    """
    try:
        bqclient.get_table(table_path)  # Make an API request.
        return True
    except NotFound:
        # print("Table {} is not found.".format(table_id))
        return False


# In[6]:


def build_new_table(bqclient, table_path, df):
    """
    build_new_table
    Accepts a path to a dimensional table, the dimension name and a data frame
    Add the surrogate key and a record timestamp to the data frame
    Inserts the contents of the dataframe to the dimensional table.
    """
    upload_bigquery_table(bqclient, table_path, "WRITE_TRUNCATE", df)


# In[7]:


def insert_existing_table( bqclient, table_path, df):
    """
    insert_existing_table
    Accepts a path to a dimensional table, the dimension name and a data frame
    Compares the new data to the existing data in the table.
    Inserts the new/modified records to the existing table
    """
    upload_bigquery_table( bqclient, table_path, "WRITE_APPEND", df)


# In[8]:


def query_bigquery_table(table_path, bqclient, surrogate_key):
    """
    query_bigquery_table
    Accepts a path to a BigQuery table and the name of the surrogate key
    Queries the BigQuery table but leaves out the update_timestamp and surrogate key columns
    Returns the dataframe
    """    
    bq_df = pd.DataFrame
    # sql_query = 'SELECT * EXCEPT ( update_timestamp, '+surrogate_key+') FROM `' + table_path + '`'
    sql_query = 'SELECT * FROM `' + table_path + '`'
    bq_df = bqclient.query(sql_query).to_dataframe()
    return bq_df


# In[9]:


def dimension_lookup( dimension_name='agency', lookup_columns=['agency', 'agency_name'], df=df):
    """
    dimension_lookup
    Lookup the lookup_columns in the dimension_name and return the associated surrogate keys
    Returns dataframe augmented with the surrogate keys
    """
    bq_df = pd.DataFrame
    surrogate_key = dimension_name+"_dim_id"
    dimension_table_path = ".".join([gcp_project,bq_dataset,dimension_name+"_dimension"])
    # Fetch the existing table
    bq_df = query_bigquery_table( dimension_table_path, bqclient, surrogate_key)
    #print(bq_df)
    # Melt the dimension dataframe into an index with the lookup columns
    m = bq_df.melt(id_vars=lookup_columns, value_vars=surrogate_key)
    #print(m)
    # Rename the "value" column to the surrogate key column name
    m=m.rename(columns={"value":surrogate_key})
    # Merge with the fact table record
    df = df.merge(m, on=lookup_columns, how='left')
    # Drop the "variable" column and the lookup columns
    df = df.drop(columns=lookup_columns)
    df = df.drop(columns="variable")
    #print(df)
    return df


# In[10]:


def date_dimension_lookup(dimension_name='date', lookup_column='created_date', df=df):
    """
    date_dimension_lookup
    Lookup the lookup_columns in a date dimension and return the associated surrogate keys
    Returns dataframe augmented with the surrogate keys
    """
    bq_df = pd.DataFrame
    surrogate_key = dimension_name+"_dim_id"
    dimension_table_path = ".".join([gcp_project,bq_dataset,dimension_name+"_dimension"])
    # Fetch the existing table
    bq_df = query_bigquery_table( dimension_table_path, bqclient, surrogate_key)
    bq_df['full_date'] = pd.to_datetime(bq_df['full_date'])
    # Return just the date portion
    bq_df["full_date"] = bq_df.full_date.dt.date

    # Dates in the 311 data look like this: 2017-08-11T11:57:00.000
    # Extract the date from 'created_date' column
    df[lookup_column] = pd.to_datetime(df[lookup_column])
    # Return just the date portion
    df[lookup_column] = df[lookup_column].dt.date

    # Melt the dimension dataframe into an index with the lookup columns
    m = bq_df.melt(id_vars='full_date', value_vars=surrogate_key)
    # Rename the "value" column to the surrogate key column name
    m=m.rename(columns={"value":lookup_column+"_dim_id"})

    # Merge with the fact table record on the created_date
    df = df.merge(m, left_on=lookup_column, right_on='full_date', how='left')

    # Drop the "variable" column and the lookup columns
    df = df.drop(columns=lookup_column)
    df = df.drop(columns="variable")
    df = df.drop(columns="full_date")
    return df


# In[11]:


if __name__ == "__main__":
    df = pd.DataFrame
    # Create the BigQuery Client
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path_to_service_account_key_file

    # Construct a BigQuery client object
    bqclient = bigquery.Client()
    
    # Load in the data file
    with open(file_source_path, 'r') as data:
            df = pd.read_csv(data)
        # Set all of the column names to lower case letters
    #print(df.head())
    df = df.rename(columns=str.lower)    
    #df.location_type = df.location_type.fillna('other')
    
    # Consider removing columns that we will never use  df.drop([....])

    # Lookup the agency dimension record  agency_dim_id
    df = dimension_lookup( dimension_name='agency', lookup_columns=['agency', 'agency_name'], df=df)

    # Lookup the location dimension record  location_dim_id
    df = dimension_lookup( dimension_name='location', lookup_columns=['borough', 'incident_zip', 'latitude', 'longitude'], df=df)

    # Lookup the channel  dimension record  channel_dim_id
    df = dimension_lookup( dimension_name='status', lookup_columns=['status'], df=df)

    # Lookup the complaint_type  dimension record  complaint_type_dim_id
    #df = dimension_lookup dimension_name='complaint_type', lookup_columns=['complaint_type', 'descriptor'], df=df)

    # Lookup the time dimension record using the time part of the created_date
    # Note - do this before looking up the date dimension
    #df = time_dimension_lookup( dimension_name='time', lookup_column='created_date', df=df)
    # The time_dimension_lookup returns a column named 'time_dim_id'. Rename this to the 'created_time_dim_id'
    #df = df.rename(columns={'time_dim_id' : 'created_time_dim_id'})

    # Lookup the created_date dimension record
    df = date_dimension_lookup(dimension_name='date', lookup_column='created_date', df=df)

    # Lookup the closed_date dimension record
    df = date_dimension_lookup(dimension_name='date', lookup_column='closed_date', df=df)
 # A list of all of the surrogate keys
    # For transaction grain, also include the 'unique_key' column
    surrogate_keys=['agency_dim_id','location_dim_id','created_date_dim_id','status_dim_id','closed_date_dim_id', 'unique_key']

    # Remove all of the other non-surrogate key columns
    df = df[surrogate_keys]

    # For daily snapshot grain we:
    # 1) Add a 'complaint_count' fact
    # 2) Use Group By to count up the number of complaints, per location, per agency, etc. per day
    # For transaction grain add in the unique_key but skip the above two steps.

    # Add a complaint count (for daily snapshot grain)
    #df['requests_count'] = 1
    # Count up the number of complaints per agency, per location, etc. per day
    #df = df.groupby(surrogate_keys)['requests_count'].agg('count').reset_index()

    # See if the target table exists
    target_table_exists = bigquery_table_exists(fact_table_path, bqclient )
    # If the target table does not exist, load all of the data into a new table
    if not target_table_exists:
        build_new_table( bqclient, fact_table_path, df)
    # If the target table exists, then perform an incremental load
    if target_table_exists:
        insert_existing_table( bqclient, fact_table_path, df)


# In[ ]:


df.head()

