import requests as R
import time
import json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, ArrayType, StructType, StructField
from pyspark.sql.functions import udf, lit


#tokens live here
import tokens

# Initialize sparksession
spark = SparkSession.builder.appName("Strava").getOrCreate()

# Set variables
onetime_code = tokens.onetime_code
client_id = tokens.client_id
client_secret = tokens.client_secret
refresh_token = tokens.refresh_token
grant_type = "authorization_code"

file = "Strava_access.json"

#def strava_access(file):
    
# create access schema
schema = StructType([StructField("token_type", StringType(), True),
                        StructField("expires_at", IntegerType(), True),
                        StructField("expires_in", IntegerType(), True),
                        StructField("refresh_token", StringType(), True ),
                        StructField("access_token", StringType(), True),
                        StructField("athlete", ArrayType(StringType()), True)
                        ])
# json_data = spark.read.option("multiline","true").json("Strava_access.json")
json_data = spark.read.schema(schema).option('multiline', True).json(file)


json_data.printSchema()

json_data.show()
new_expiry_at = json_data.select('expires_at').collect()[0][0]
new_expiry_in = json_data.select('expires_in').collect()[0][0]
new_access_token = json_data.select('access_token').collect()[0][0]
new_refresh_token = json_data.select('refresh_token').collect()[0][0]

print(new_expiry_at)

#decide whether to use current credentials or refresh

#def access_token(json_path, client_secret):
#set current system time as unix epoch, subtract 1 hour

# create access schema
schema = StructType([StructField("token_type", StringType(), True),
                     StructField("expires_at", IntegerType(), True),
                     StructField("expires_in", IntegerType(), True),
                     StructField("refresh_token", StringType(), True ),
                     StructField("access_token", StringType(), True),
                     StructField("athlete", ArrayType(StringType()), True)
                    ])

# json_data = spark.read.option("multiline","true").json("Strava_access.json")
access_data = spark.read.schema(schema).option('multiline', True).json(json_path)

expiry_at = access_data.select('expires_at').collect()[0][0]
expiry_in = access_data.select('expires_in').collect()[0][0]
access_token = access_data.select('access_token').collect()[0][0]
refresh_token = access_data.select('refresh_token').collect()[0][0]
athlete = access_data.select('athlete').collect()[0][0]

curtime = time.time()
thresholdtime = curtime - 3600
if expiry_at > thresholdtime:
    #if token needs to be refreshed then
    # set payload for refresh
    payload_r = {f"client_id":{client_id},f"client_secret":{client_secret},f"grant_type":{grant_type},f"refresh_token":{refresh_token}}
    # post onetime access code and catch returned json. 
    refresh_url = R.post("https://www.strava.com/oauth/token", params=payload_r)
    #create access schema
    schema_r = StructType([StructField("token_type", StringType(), True),
                    StructField("access_token", StringType(), True),
                    StructField("expires_at", IntegerType(), True),
                    StructField("expires_in", IntegerType(), True),
                    StructField("refresh_token", StringType(), True )
                    ])
    # json_data = spark.read.option("multiline","true").json("Strava_access.json")
    json_data_refresh = spark.read.schema(schema_r).option('multiline', True).json(refresh_url)
    spark.write.json('./access_tokens.json').method('overwrite')


    token_type = json_data_refresh.select('token_type').collect()[0][0]
    expiry_at = json_data_refresh.select('expires_at').collect()[0][0]
    expiry_in = json_data_refresh.select('expires_in').collect()[0][0]
    access_token = json_data_refresh.select('access_token').collect()[0][0]
    refresh_token = json_data_refresh.select('refresh_token').collect()[0][0]
    athlete_data = {"athlete":athlete}
    athlete_append = athlete_data.loads(athlete_data)
    json_data_refresh.append(athlete_append)
    json_data_refresh.write.format("json").mode("overwrite").path(json_path)
print(refresh_url)

""" 
def access_token(refresh_token):
    curtime = time.time()
    thresholdtime = curtime - 3600

    if refresh_token > thresholdtime:
        #add in function to refresh tokens
        else
        
 """