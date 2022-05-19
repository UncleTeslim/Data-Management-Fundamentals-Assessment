import pandas as pd
import json


#read csv file
clean_csv = pd.read_csv("clean.csv", low_memory=False) 
#pandas dataframe
df = pd.DataFrame(clean_csv) 
#makes json from dataframe
clean_csv_json = df.to_json(   orient = "records", 
                        date_format = "epoch", 
                        double_precision = 10, 
                        force_ascii = True, 
                        date_unit = "ms", 
                        default_handler = None
                    ) 
#converts the json string to a list of python dictionary objects
clean_csv_list = json.loads(clean_csv_json)
# slice first 100 from the list
clean_csv_list = clean_csv_list[:100] 


with open('insert-100.sql', 'w'):
    for row in clean_csv_list:
        if row['Current']==True:
            current = 1 
        elif row["Current"]==None:
            current = "NULL"
        else:
            current = 0
        print(  f"""INSERT INTO `readings` (`Date Time`,`NOx`,`NO2`,`NO`,`SiteID`,`PM10`,`NVPM10`,`VPM10`,`NVPM2.5`,`PM2.5`,`VPM2.5`,`CO`,`O3`,`SO2`,`Temperature`,`RH`,`AirPressure`,`Location`,`geo_point_2d`,`DateStart`,`DateEnd`,`Current`,`Instrument Type`) VALUES (""" + 
                str(f"""'{row['Date Time']}'""" if row['Date Time'] else "NULL")+","+
                str(row['NOx'] if row['NOx'] else "NULL")+","+
                str(row['NO2'] if row['NO2'] else "NULL")+ ","+
                str(row['NO'] if row['NO'] else "NULL")+","+
                str(row['SiteID'] if row['SiteID'] else "NULL")+","+
                str(row['PM10'] if row['PM10'] else "NULL")+","+
                str(row['NVPM10'] if row['NVPM10'] else "NULL")+","+
                str(row['VPM10'] if row['VPM10'] else "NULL")+","+
                str(row['NVPM2.5'] if row['NVPM2.5'] else "NULL")+","+
                str(row['PM2.5'] if row['PM2.5'] else "NULL")+","+
                str(row['VPM2.5'] if row['VPM2.5'] else "NULL")+","+
                str(row['CO'] if row['CO'] else "NULL")+","+
                str(row['O3'] if row['O3'] else "NULL")+","+
                str(row['SO2'] if row['SO2'] else "NULL")+","+
                str(row['Temperature'] if row['Temperature'] else "NULL")+","+
                str(row['RH'] if row['RH'] else "NULL")+","+
                str(row['Air Pressure'] if row['Air Pressure'] else "NULL")+ ","+
                str(f"""'{row['Location']}'""" if row['Location'] else "NULL")+","+
                str(f"""'{row['geo_point_2d']}'""" if row['geo_point_2d'] else "NULL")+","+
                str(f"""'{row['DateStart']}'""" if row['DateStart'] else "NULL")+","+
                str(f"""'{row['DateEnd']}'""" if row['DateEnd'] else "NULL")+","+
                str(current)+","+
                str(f"""'{row['Instrument Type']}'""" if row['Instrument Type'] else "NULL")+
                ");", file=open('insert-100.sql', 'a')) #prints insertion commands with the first 100 data to the insert-100.sql file
