import pymongo
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
from flatten_json import flatten
from sqlalchemy import create_engine , text
import pymysql
import numpy as np
# import redshift_connector
import psycopg2
import os
import pytz
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
import boto3
import s3fs
# import datetime
import io
import requests
import os




MONGO_CONNECTION_STRING = os.getenv("MONGO_URI")
MONGO_DATABASE = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("AWS_REGION")
s3_bucket_name = os.getenv("S3_BUCKET_NAME")
S3_PARTITION_PREFIX = os.getenv("S3_PREFIX")



# # Calculate the start date as 10 days ago from the current date
end_date = datetime.now()
start_date = end_date - timedelta(days=10)


# # Define start and end date range
# start_date = datetime(2023, 1, 1)
# end_date = datetime.now()


# Query to retrieve data for the specified date range
query = {
    "$or": [
        {"updatedAt": {"$gte": start_date, "$lte": end_date}},
        {"createdAt": {"$gte": start_date, "$lte": end_date}}
    ]
}

# Retrieve data for the specified date range
cursor = list(mongo_collection.find(query))

# Convert the cursor to a Pandas DataFrame
df = pd.DataFrame(cursor)

dict_flattened = (flatten(record, '.') for record in cursor)
df = pd.DataFrame(dict_flattened)


columns_to_select = [
'_id','name','phone.0','admins.0.email','type','businessCategory','industry','businessTier','status.value','country.name','address.0.city.name','address.0.country.name','paymentInfo.paymentType'
                ,'paymentInfo.paymentFrequency','paymentInfo.paymentSchedule.4','paymentInfo.paymentTransferMethod.0','paymentInfo.creditLimit','payment.pricingModel','payment.codPrice',
                'payment.minPrice','payment.basePrice','payment.kmPrice','payment.fixedPrice','payment.sameDayPrice','payment.nextDayPrice','payment.sameDayLongDistance'
                ,'pricePlaneActiveDate','pricingTier.name','pricingTier.changeType','pricingFlags.isZeroCodDiscountApplied','pricingFlags.isExtraCodFeesApplied',
                'pricingFlags.isExpediteFeesApplied','pricingFlags.isInsuranceFeesApplied','pricingFlags.isPosFeesApplied','pricingFlags.isCodFeesApplied',
                'bankInfo.accountNumber','bankInfo.bankName','bankInfo.ibanNumber','salesManager.name','accountManager.name','isFirstOrderCreated',
                'isFirstPickupPickedUp','firstPickupPickedUpAt','fulfillmentInfo.isActive','flyersInfo.availableNoOfFlyers','flyersInfo.isLimited','flyersInfo.maxLimitOfFlyers','flyersInfo.extraQuota',
                'flyersInfo.isExtraQuotaAdded','flyersInfo.lastUpdated','createdAt','updatedAt','bankInfo.lastUpdate','regSrc','fullyActivatedAt','insurancePlan.name','insurancePlan.assignedAt']


# Check which columns are not in the DataFrame
missing_columns = [col for col in columns_to_select if col not in df.columns]

# Add missing columns with empty values
for col in missing_columns:
    df[col] = pd.Series(dtype='object')  # Add empty column with object dtype

# Now you can select the desired columns
df_selected = df[columns_to_select]

df_selected = df_selected.copy()

df_selected['deleted'] = False

df_selected.columns = [col.replace('.', '_') for col in df_selected.columns]
df_selected.columns = df_selected.columns.str.replace('__', '_')

# Rename columns 'createdAt' and 'updatedAt' to 'created_at' and 'updated_at'
df_selected = df_selected.copy()
# Rename columns in df_selected
df_selected = df_selected.rename(columns={'_id' : 'business_id','phone_0':'phone','admins_0_email':'email','businessCategory':'category',
                                        'businessTier':'business_tier','status_value':'status','address_0_city_name':'default_pickup_location_city','address_0_country_name':'default_pickup_location_country',
                                        'paymentInfo_paymentType':'payment_type','paymentInfo_paymentFrequency':'payment_frequency','paymentInfo_paymentSchedule_4':'payment_schedule',
                                        'paymentInfo_paymentTransferMethod_0':'payment_transfer_method','paymentInfo_creditLimit':'payment_credit_limit','payment_pricingModel':'payment_pricing_model',
                                        'payment_codPrice':'payment_cod_price','payment_minPrice':'payment_min_price','payment_basePrice':'payment_base_price',
                                        'payment_kmPrice':'payment_km_price','payment_fixedPrice':'payment_fixed_price','payment_sameDayPrice':'payment_same_day_price','payment_nextDayPrice':'payment_next_day_price',
                                        'payment_sameDayLongDistance':'payment_same_day_long_distance','pricePlaneActiveDate':'price_plan_active_date','old_pricing_tier_name':'old_pricing_tier_name','pricingTier_name':'pricing_tier_name',
                                        'pricingTier_changeType':'pricing_tier_change_type','pricingFlags_isZeroCodDiscountApplied':'pricing_tier_is_zero_cod_discount_applied','pricingFlags_isExtraCodFeesApplied':'pricing_tier_is_extra_cod_fees_applied',
                                        'pricingFlags_isExpediteFeesApplied':'pricing_tier_is_expedite_fees_applied','pricingFlags_isInsuranceFeesApplied':'pricing_tier_is_insurance_fees_applied',
                                        'pricingFlags_isPosFeesApplied':'pricing_tier_is_pos_fees_applied','pricingFlags_isCodFeesApplied':'pricing_tier_is_cod_fees_applied','bankInfo_accountNumber':'bankInfo_account_number',
                                        'bankInfo_bankName':'bankInfo_bank_name','bankInfo_ibanNumber':'bankInfo_bank_iban','salesManager_name':'sales_manager_name',
                                        'accountManager_name':'account_manager_name','isFirstOrderCreated':'is_first_order_created','isFirstPickupPickedUp':'is_first_pickup_created','isFirstPickupPickedUp':'is_first_pickup_picked_up','firstPickupPickedUpAt':'firstPickupPickedUpAt',
                                        'fulfillmentInfo_isActive':'is_fulfillmentInfo_active','flyersInfo_availableNoOfFlyers':'flyersInfo_available_no_of_flyers','flyersInfo_isLimited':'flyersInfo_is_limited','flyersInfo_maxLimitOfFlyers':'flyersInfo_max_limit_of_flyers',
                                        'flyersInfo_extraQuota':'flyersInfo_extra_quota','flyersInfo_isExtraQuotaAdded':'flyersInfo_is_extra_quota_added','flyersInfo_lastUpdated':'flyersInfo_last_updated',
                                        'regSrc':'regSrc',
                                        'fullyActivatedAt':'fully_activatedAt'})


# Define the columns and their corresponding data types
data_types = {
    "business_id": 'str',
    "name": 'str',
    "phone": 'str',
    "email": 'str',
    "type": 'str',
    "category": 'str',
    "industry": 'str',
    "business_tier": 'str',
    "status": 'str',
    "country_name": 'str',
    "default_pickup_location_city": 'str',
    "default_pickup_location_country": 'str',
    "payment_type": 'str',
    "payment_frequency": 'str',
    "payment_schedule": 'str',
    "payment_transfer_method": 'str',
    "payment_credit_limit": 'str',
    "payment_pricing_model": 'str',
    "payment_cod_price": 'float64',
    "payment_min_price": 'float64',
    "payment_base_price": 'float64',
    "payment_km_price": 'float64',
    "payment_fixed_price": 'float64',
    "payment_same_day_price": 'float64',
    "payment_next_day_price": 'float64',
    "payment_same_day_long_distance": 'float64',
    "price_plan_active_date": 'datetime64[ns]',
    "pricing_tier_name": 'str',
    "pricing_tier_change_type": 'str',
    "pricing_tier_is_zero_cod_discount_applied": bool,
    "pricing_tier_is_extra_cod_fees_applied": bool,
    "pricing_tier_is_expedite_fees_applied": bool,
    "pricing_tier_is_insurance_fees_applied": bool,
    "pricing_tier_is_pos_fees_applied": bool,
    "pricing_tier_is_cod_fees_applied": bool,
    "bankInfo_account_number": 'str',
    "bankInfo_bank_name": 'str',
    "bankInfo_bank_iban": 'str',
    "sales_manager_name": 'str',
    "account_manager_name": 'str',
    "is_first_order_created": bool,
    "is_first_pickup_picked_up": bool,
    "firstPickupPickedUpAt": 'datetime64[ns]',
    "is_fulfillmentInfo_active": bool,
    "flyersInfo_available_no_of_flyers": 'int',
    "flyersInfo_is_limited": bool,
    "flyersInfo_max_limit_of_flyers": 'int',
    "flyersInfo_extra_quota": 'int',
    "flyersInfo_is_extra_quota_added": bool,
    "flyersInfo_last_updated": 'datetime64[ns]',
    "createdAt": 'datetime64[ns]',
    "updatedAt": 'datetime64[ns]',
    "bankInfo_last_date": 'datetime64[ns]',
    "regSrc": 'str',
    "fully_activatedAt": 'datetime64[ns]',
    "deleted":bool,
    "account":'str',
    "type_of_account":"str",
    "insurancePlan_name":str,
    "insurancePlan_assignedAt":'datetime64[ns]'
}

df_selected = df_selected.copy()


# Add missing columns to DataFrame with empty string as initial value
for column_name in data_types.keys():
    if column_name not in df_selected.columns:
        df_selected[column_name] = ''

# Replace NaN values with 0 in columns that are going to be converted to integer or float
int_float_columns = [col for col, dtype in data_types.items() if dtype in ['int','int64' , 'float64' , 'float']]
df_selected[int_float_columns] = df_selected[int_float_columns].fillna(0)


# Convert columns to specified data types
df_selected = df_selected.astype(data_types)


import pytz
from datetime import timedelta
import pandas as pd

# Define time zones for Egypt and KSA
egypt_tz = pytz.timezone('Africa/Cairo')
ksa_tz = pytz.timezone('Asia/Riyadh')
uae_tz = pytz.timezone('Asia/Dubai')

# Function to convert UTC time to local time based on location and format it
def convert_to_local_time(utc_time, location):

    if pd.isnull(utc_time):
        return None  # Return None if utc_time is NaT

    if location is None:
        return None  # Return None if location is None

    if location.lower() == 'egypt':
        local_time = utc_time.astimezone(egypt_tz)
    elif location.lower() == 'saudi arabia':
        local_time = utc_time.astimezone(ksa_tz)
    elif location.lower() == 'united arab emirates':
        local_time = utc_time.astimezone(uae_tz)
    else:
        return None

    # Format the local time as a string with the original time components
    formatted_time = local_time.strftime('%Y-%m-%d %H:%M:%S.%f')

    return formatted_time


# Localize 'created_at' and 'updated_at' columns to UTC if they are not already in UTC
df_selected['createdAt'] = pd.to_datetime(df_selected['createdAt']).dt.tz_localize('UTC')
df_selected['updatedAt'] = pd.to_datetime(df_selected['updatedAt']).dt.tz_localize('UTC')
df_selected['price_plan_active_date'] = pd.to_datetime(df_selected['price_plan_active_date']).dt.tz_localize('UTC')
df_selected['firstPickupPickedUpAt'] = pd.to_datetime(df_selected['firstPickupPickedUpAt']).dt.tz_localize('UTC')
df_selected['flyersInfo_last_updated'] = pd.to_datetime(df_selected['flyersInfo_last_updated']).dt.tz_localize('UTC')
df_selected['bankInfo_last_date'] = pd.to_datetime(df_selected['bankInfo_last_date']).dt.tz_localize('UTC')
df_selected['fully_activatedAt'] = pd.to_datetime(df_selected['fully_activatedAt']).dt.tz_localize('UTC')
df_selected['insurancePlan_assignedAt'] = pd.to_datetime(df_selected['insurancePlan_assignedAt']).dt.tz_localize('UTC')


# Iterate through the DataFrame and adjust 'createdAt' and 'updatedAt' columns
df_selected['createdAt'] = df_selected.apply(lambda row: convert_to_local_time(row['createdAt'], row['country_name']), axis=1)
df_selected['updatedAt'] = df_selected.apply(lambda row: convert_to_local_time(row['updatedAt'], row['country_name']), axis=1)
df_selected['price_plan_active_date'] = df_selected.apply(lambda row: convert_to_local_time(row['price_plan_active_date'], row['country_name']), axis=1)
df_selected['firstPickupPickedUpAt'] = df_selected.apply(lambda row: convert_to_local_time(row['firstPickupPickedUpAt'], row['country_name']), axis=1)
df_selected['flyersInfo_last_updated'] = df_selected.apply(lambda row: convert_to_local_time(row['flyersInfo_last_updated'], row['country_name']), axis=1)
df_selected['bankInfo_last_date'] = df_selected.apply(lambda row: convert_to_local_time(row['bankInfo_last_date'], row['country_name']), axis=1)
df_selected['fully_activatedAt'] = df_selected.apply(lambda row: convert_to_local_time(row['fully_activatedAt'], row['country_name']), axis=1)
df_selected['insurancePlan_assignedAt'] = df_selected.apply(lambda row: convert_to_local_time(row['insurancePlan_assignedAt'], row['country_name']), axis=1)


# Convert the columns to datetime format (in case they are not already)
datetime_columns = ['createdAt', 'updatedAt', 'price_plan_active_date', 'firstPickupPickedUpAt', 'flyersInfo_last_updated',
                    'bankInfo_last_date', 'fully_activatedAt','insurancePlan_assignedAt']

df_selected[datetime_columns] = df_selected[datetime_columns].apply(pd.to_datetime)

# Replace NaN values, 'nan', and 'NAN' strings with '' in columns that are going to be converted to string
str_columns = [col for col, dtype in data_types.items() if dtype == 'str']
for col in str_columns:
    df_selected[col].replace({np.nan: '', 'nan': '', 'NAN': '' , 'NaN' : ''}, inplace=True)


# Specify the columns you want to truncate to 60 characters
columns_to_truncate = ['name', 'type', 'category', 'industry','business_tier','status','country_name','default_pickup_location_city','default_pickup_location_country','payment_type',
                    'payment_frequency','payment_schedule','payment_transfer_method','payment_pricing_model','pricing_tier_name','pricing_tier_change_type',
                    'bankInfo_bank_iban', 'account_manager_name','regSrc']


# Truncate the string in the 'receiver_fullName' column to the first 20 characters
df_selected[columns_to_truncate] = df_selected[columns_to_truncate].apply(lambda x: x.str.slice(0, 20))

# Truncate the 'bankInfo_bank_name' column to 90 characters
df_selected['sales_manager_name'] = df_selected['sales_manager_name'].str.strip().str.slice(0, 40)

# Truncate the 'bankInfo_bank_name' column to 90 characters
df_selected['bankInfo_bank_name'] = df_selected['bankInfo_bank_name'].str.strip().str.slice(0, 30)

# Function to get the first phone number
def get_first_phone_number(phone):
    return phone.split('-')[0]

# Apply the function to the sender_phone column
df_selected['phone'] = df_selected['phone'].apply(get_first_phone_number)


df_selected = df_selected.drop(columns=['bankInfo_lastUpdate'])



import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime
import io



# Create a session and S3 client
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)
s3 = session.client('s3')

# Get yesterday's date
yesterday = datetime.date.today() - datetime.timedelta(days=1)
partition_path = f"{S3_PARTITION_PREFIX}/date={yesterday}/"

# Convert Pandas DataFrame to Parquet in memory
table = pa.Table.from_pandas(df_selected)
buffer = io.BytesIO()
pq.write_table(table, buffer, compression='snappy')
buffer.seek(0)

# Upload Parquet file to S3
file_name = f"data_{yesterday}.parquet"
s3_key = f"{partition_path}{file_name}"

s3.put_object(
    Bucket=s3_bucket_name,
    Key=s3_key,
    Body=buffer.getvalue()
)

print(f"✅ Parquet file uploaded to S3://{s3_bucket_name}/{s3_key}")

# Close the MongoDB connection