from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import pandas
import requests
import datetime

#    .config("spark.executor.memory","4g")\
#    .config("spark.executor.cores","2")\
#    .config("spark.driver.memory","6g")\
#    .config("spark.yarn.access.hadoopFileSystems","s3a://prod-cdptrialuser19-trycdp-com")\

spark = SparkSession\
    .builder\
    .appName("Data Push Job")\
    .master("local[*]") \
    .config("spark.driver.memory","4g")\
    .config("spark.hadoop.yarn.resourcemanager.principal","trial1901")\
    .getOrCreate()
    

payload = {
    'grant_type': 'client_credentials', 
    'client_id': os.environ.get('CLIENT_ID'),
    'client_secret' : os.environ.get('CLIENT_SECRET'),
    'scope' : 'service:iot-hub-prod:t89941b50e50f470f937aab2c43aadcf6_hub/full-access'
}

r = requests.post("https://access.bosch-iot-suite.com/token", data=payload)

access_token = r.json()["access_token"]

things_url = "https://things.eu-1.bosch-iot-suite.com/api/2/search/things?namespaces=io.bosch.bcx2020"
auth_header = {"Authorization": "Bearer " + access_token}
r = requests.get(things_url,headers=auth_header)

data_set = r.json()
data_point_parkingsensor = []
data_point_traci = []
data_point_xdf = []

for data_points in data_set['items']:

  print(data_points['thingId'])
  if '@type' in data_points['attributes']:
    print(data_points['attributes']['@type'])

    ## parking sensor
    if data_points['attributes']['@type'] == "bcds-parkingsensor":

      try:
        data_point_parkingsensor.append({
          'thingId' : data_points['thingId'],
          'time_stamp' : int(datetime.datetime.now().strftime("%s")),
          'last_seen' : int(data_points['features']["lastSeen"]["properties"]["status"]["value"]["value"]),
          'parking_state' : int(data_points['features']["parkingState"]["properties"]["status"]["value"]["value"]),
          'dev_id' : data_points['features']["payload"]["properties"]["status"]["value"]["value"]["dev_id"],
          'hardware_serial' : data_points['features']["payload"]["properties"]["status"]["value"]["value"]["hardware_serial"]
        })
      except:
        print("Unable to process - malformed data")  
      
    ## traci
    if data_points['attributes']['@type'] == "traci":
      try:
        data_point_traci.append({
          'thingId' : data_points['thingId'],
          'time_stamp' : int(datetime.datetime.now().strftime("%s")),
          'last_seen' : int(data_points['features']["lastSeen"]["properties"]["status"]["value"]["value"]),
          'longitude' : int(data_points['features']["location"]["properties"]["status"]["value"]["longitude"]),
          'latitude' : int(data_points['features']["location"]["properties"]["status"]["value"]["latitude"]),
          'altitude' : int(data_points['features']["location"]["properties"]["status"]["value"]["altitude"]),
          'magnetic_strength_mx' : int(data_points['features']["magneticStrength"]["properties"]["status"]["value"]["mx"]),
          'magnetic_strength_my' : int(data_points['features']["magneticStrength"]["properties"]["status"]["value"]["my"]),
          'magnetic_strength_mz' : int(data_points['features']["magneticStrength"]["properties"]["status"]["value"]["mz"]),
          'temperature' : int(data_points['features']["temperature"]["properties"]["status"]["value"]["temperature"])
        })
      except:
        print("Unable to process - malformed data")        

    ## bcds-xdk
    if data_points['attributes']['@type'] == "bcds-xdk":
      try:
        data_point_xdf.append({
          'thingId' : data_points['thingId'],
          'time_stamp' : int(datetime.datetime.now().strftime("%s")),
          'last_seen' : int(data_points['features']["lastSeen"]["properties"]["status"]["value"]["value"]),
          'acceleration_ax': int(data_points['features']["acceleration"]["properties"]["status"]["value"]["ax"]),
          'acceleration_ay': int(data_points['features']["acceleration"]["properties"]["status"]["value"]["ay"]),
          'acceleration_az': int(data_points['features']["acceleration"]["properties"]["status"]["value"]["az"]),
          'magnetic_strength_mx': int(data_points['features']["magneticStrength"]["properties"]["status"]["value"]["mx"]),
          'magnetic_strength_my': int(data_points['features']["magneticStrength"]["properties"]["status"]["value"]["my"]),
          'magnetic_strength_mz': int(data_points['features']["magneticStrength"]["properties"]["status"]["value"]["mz"]),
          'magnetic_strength_mr': int(data_points['features']["magneticStrength"]["properties"]["status"]["value"]["mr"]),
          'illuminance': int(data_points['features']["illuminance"]["properties"]["status"]["value"]["illuminance"]),
          'rotation_gx': int(data_points['features']["rotation"]["properties"]["status"]["value"]["gx"]),
          'rotation_gy': int(data_points['features']["rotation"]["properties"]["status"]["value"]["gy"]),
          'rotation_gz': int(data_points['features']["rotation"]["properties"]["status"]["value"]["gz"]),
          'temperature': int(data_points['features']["temperature"]["properties"]["status"]["value"]["temperature"]),
          'humidity': int(data_points['features']["humidity"]["properties"]["status"]["value"]["humidity"]),
          'pressure': int(data_points['features']["pressure"]["properties"]["status"]["value"]["pressure"])
        })
      except:
        print("Unable to process - malformed data")  

# write the lists to         
spark.createDataFrame(pandas.DataFrame(data_point_parkingsensor))\
  .write.mode("append").saveAsTable("bosch.parkingsensor")
  
spark.createDataFrame(pandas.DataFrame(data_point_traci))\
  .write.mode("append").saveAsTable("bosch.traci")
  
spark.createDataFrame(pandas.DataFrame(data_point_xdf))\
  .write.mode("append").saveAsTable("bosch.xdk")
      
spark.stop()