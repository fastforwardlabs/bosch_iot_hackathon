import pandas
import requests

## Bosch IoT Insights
url = 'https://bosch-iot-insights.com/mongodb-query-service/v2/zz1726233902111/collections'
SFDE_USERNAME = os.environ.get('SFDE_USERNAME')
SFDE_PASSWORD = os.environ.get('SFDE_PASSWORD')
r = requests.get(url, auth=(SFDE_USERNAME, SFDE_PASSWORD))

r.json()

## Bosch IoT Suite

#### Step 1: get the access token
payload = {
    'grant_type': 'client_credentials', 
    'client_id': os.environ.get('CLIENT_ID'),
    'client_secret' : os.environ.get('CLIENT_SECRET'),
    'scope' : 'service:iot-hub-prod:t89941b50e50f470f937aab2c43aadcf6_hub/full-access'
}

r = requests.post("https://access.bosch-iot-suite.com/token", data=payload)

access_token = r.json()["access_token"]

#### Step 2: Get Some Data

things_url = "https://things.eu-1.bosch-iot-suite.com/api/2/search/things?namespaces=io.bosch.bcx2020"
auth_header = {"Authorization": "Bearer " + access_token}
r = requests.get(things_url,headers=auth_header)

#### Step 3: Display Data

data_set = r.json()
accel = data_set['items'][18]['features']['acceleration']['properties']['status']['value']
pandas.DataFrame.from_dict(accel,orient='index')

specific_url = things_url + "&filter=ge%28thingId%2C%22io.bosch.bcx2020%3AXDK-F4-B8-5E-3E-A7-8D%22%29"
r = requests.get(specific_url,headers=auth_header)

magnetic = r.json()['items'][0]['features']['magneticStrength']['properties']['status']['value']
pandas.DataFrame.from_dict(magnetic,orient='index')
