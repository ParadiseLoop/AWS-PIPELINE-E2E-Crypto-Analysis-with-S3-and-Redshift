#Python Script that has the purpose of further processing of the previously loaded and transformed input datasets.
#The script convers Price columns into floats and merges the two datasets "BTC_EUR_Historical_Data" and
#"google_trend_bitcoin" on the Date column.

import boto3
import pandas as pd
import os

# Definition of the various S3 buckets and the file names
input_bucket = "silver-bucket-dani"
output_bucket = "gold-bucket-dani"
btc_file = "BTC_EUR_Historical_Data.parquet"
google_file = "google_trend_bitcoin.parquet"

# Here I initialize the S3 client
s3 = boto3.client('s3')

# Creation of a custom function to download a Parquet file from S3 and load it into a DataFrame
def load_parquet_from_s3(bucket, key, temp_filename):
    s3.download_file(bucket, key, temp_filename)
    df = pd.read_parquet(temp_filename)
    os.remove(temp_filename)
    return df

# Temporary file paths for loading
temp_file_btc = "/tmp/BTC_EUR_Historical_Data.parquet"
temp_file_google = "/tmp/google_trend_bitcoin.parquet"

# Load the BTC and Google Trends data from the silver bucket
df_btc = load_parquet_from_s3(input_bucket, btc_file, temp_file_btc)
df_google = load_parquet_from_s3(input_bucket, google_file, temp_file_google)

# Converting `Price` to numeric to avoid aggregation errors and coercing errors to NaN
df_btc['Price'] = pd.to_numeric(df_btc['Price'].str.replace(",", ""))

# Calculating 10-day moving average for the `Price` column in BTC data
df_btc['Price'] = df_btc['Price'].rolling(window=10, min_periods=1).mean()


# Joining BTC data with Google Trends data on `Date`
df_merged = pd.merge(df_btc, df_google, on="Date", how="inner")

# Renaming the `interesse bitcoin` column to `indice_google_trend`
df_merged = df_merged[['Date', 'Price', 'interesse bitcoin']].rename(columns={'interesse bitcoin': 'indice_google_trend'})

# Creation of a custom function to save a DataFrame in Parquet format and upload it to S3
def save_df_to_parquet_and_upload(df, bucket, key, temp_filename):
    df.to_parquet(temp_filename, index=False)
    s3.upload_file(Filename=temp_filename, Bucket=bucket, Key=key)
    os.remove(temp_filename)

# Defining temporary file path for the final Parquet file
temp_file_output = "/tmp/BTC_Google_Trends_Merged.parquet"
output_file = "BTC_Google_Trends_Merged.parquet"

# Saving and uploading the final merged DataFrame to the gold bucket
save_df_to_parquet_and_upload(df_merged, output_bucket, output_file, temp_file_output)

print("Data processing completed and saved to gold-bucket-dani.")
