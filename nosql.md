# Database : **MongoDB**


Field | Description
------|------------
_id | The unique Id designated by Mongodb 
Date Time | A Timestamp field
NOx | A Double field
NO2 | A Double field
NO | A Double field
SiteID | An Integer Field
PM10 | A Double field
NVPM10 | A Double field
VPM10 | A Double field
NVPM2.5 | A Double field
PM2.5 | A Double field
VPM2.5 | A Double field
CO | A Double field
O3 | A Double field
SO2 | A Double field
Temperature | A Double field
RH | A Double field
AirPressure | A Double field
Location | A String field
geo_point_2d | A String field
DateStart | A Timestamp field
DateEnd | A Timestamp field
Current | A Boolean field
Instrument Type | A String field





# IMPLEMENTATION USING PYMONGO


#installing libraries in terminal
- `pip install pymongo`
- `pip install pandas`



```python
#importing libraries
from pymongo import MongoClient
import pandas as pd
import json


#connect to mongo db 
conn = MongoClient("mongodb://localhost:27017/replicaSet=rs") 

#connect to or create new database
db = conn["pollution-db"] 


#read csv file from working directory
clean_csv = pd.read_csv("clean.csv", low_memory=False) 

#convert to dataframe
df = pd.DataFrame(clean_csv)

#convert to json
clean_csv_json = df.to_json(   orient = "records", 
                        date_format = "epoch", 
                        double_precision = 10, 
                        force_ascii = True, 
                        date_unit = "ms", 
                        default_handler = None
                    ) 

#converts json to dictionary list
clean_csv_list = json.loads(clean_csv_json) 


#Check for location and insert readings
for datu in clean_csv_list: 
    if data["Location"] == 'Bath Road': 
        db.readings.insert_one(datua) 

```

#QUERYING THE DB

```python
from bson.objectid import ObjectId

document = db.readings.find_one({"_id": ObjectId('61d2c09dbac2b8724e9f1116')})

print(document)

```


