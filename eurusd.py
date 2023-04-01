import pandas as pd
import requests
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from IPython.display import display

key_path="sa-credentials.json"
project_id="ordinal-bucksaw-380920"
dataset_id="forex"
table="eurusd"
table_id="{}.{}.{}".format(project_id, dataset_id, table)
print("TABLE: ", table_id)


# GCP service account credentials

credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

client = bigquery.Client(credentials=credentials, project=project_id)


# Get data from API

url = "https://twelve-data1.p.rapidapi.com/time_series"

querystring = {"symbol":"EUR/USD","interval":"1day","outputsize":"10","format":"json"} # outputsize=number of rows

headers = {
	"X-RapidAPI-Key": "8c5dc4ddbcmsh6d77993cedb4799p19275cjsna60f700abbca",
	"X-RapidAPI-Host": "twelve-data1.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers, params=querystring)

data = response.json()

# Create and display the dataframe

df = pd.json_normalize(response.json(), record_path =['values'])

display(df)

# Load data into GCP

def load_table_dataframe(key_path,project_id,table_id):

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE")

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )  
    job.result()  

    data = client.get_table(table_id)  
    return data


data = load_table_dataframe(key_path,project_id, table_id)

# Query data from the table

table = client.get_table(table_id)

df1 = client.list_rows(table).to_dataframe()

print('Data from the BQ table:')
display(df1)


    