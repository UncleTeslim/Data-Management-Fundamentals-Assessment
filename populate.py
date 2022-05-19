#imported necessary libraries( pandas, sql connector and json)
import pandas as pd
import mysql.connector as sql 
import json



#READING CSV FILE FROM WORKING DIRECTORY AND CONVERTING TO DICTIONARY

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


##connecting to the DB
mydb = sql.connect(
    host= "localhost",      
    user="root",            
    password= "testes"  
)                            

cursor = mydb.cursor()

#create new database
cursor.execute("CREATE DATABASE `pollution-db2` ") 


#after creating the database above (lines 26-36), I selected the DB here since i would be working with it
mydb = sql.connect(
    host= "localhost",      
    user="root",            
    password= "testes",  
    database= "pollution-db2"   
)
mycursor = mydb.cursor()



#drop reading table if it already exists in the db
mycursor.execute("DROP TABLE IF EXISTS `readings`;") 

#create new reading tabke if it doesn't exist
mycursor.execute(""" CREATE TABLE readings (   
    id INTEGER(11) NOT NULL AUTO_INCREMENT,
    `Date Time` DATETIME,
    NOx FLOAT(10),
    NO2 FLOAT(10),
    NO FLOAT(10),
    SiteID INTEGER(10),
    PM10 FLOAT(10),
    NVPM10 FLOAT(10),
    VPM10 FLOAT(10),
    `NVPM2.5` FLOAT(10),
    `PM2.5` FLOAT(10),
    `VPM2.5` FLOAT(10),
    CO FLOAT(10),
    O3 FLOAT(10),
    SO2 FLOAT(10),
    Temperature FLOAT(10),
    RH FLOAT(10),
    AirPressure FLOAT(10),
    Location VARCHAR(255),
    geo_point_2d VARCHAR(255),
    DateStart DATETIME,
    DateEnd DATETIME,
    Current BOOLEAN,
    `Instrument Type` VARCHAR(255),
    primary key (id)
    )""") #Creates a table in the database Instance


#order in which to insert data into db
insert_data = """INSERT INTO readings (
            `Date Time`,
            NOx,
            NO2,
            NO,
            SiteID,
            PM10,
            NVPM10,
            VPM10,
            `NVPM2.5`,
            `PM2.5`,
            `VPM2.5`,
            CO,
            O3,
            SO2,
            Temperature,
            RH,
            AirPressure,
            Location,
            geo_point_2d,
            DateStart,
            DateEnd,
            Current,
            `Instrument Type`
            ) VALUES (  %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, 
                        %s, %s, %s)""" 
#inserts new rows into the table from the list
for row in clean_csv_list:
    mycursor.execute(insert_data, ( row['Date Time'], row['NOx'], row['NO2'], row['NO'], row['SiteID'], 
                                row['PM10'], row['NVPM10'], row['VPM10'], row['NVPM2.5'], row['PM2.5'], 
                                row['VPM2.5'], row['CO'], row['O3'], row['SO2'], row['Temperature'], 
                                row['RH'], row['Air Pressure'], row['Location'], row['geo_point_2d'], row['DateStart'], 
                                row['DateEnd'], row['Current'], row['Instrument Type'])  ) 



mydb.commit()


#drop schema table if it exists already
mycursor.execute("DROP TABLE IF EXISTS `schema_table`;") 

#creates new schema table if not existing
mycursor.execute(""" CREATE TABLE schema_table (
                    `measure` VARCHAR(255),
                    `desc` VARCHAR(255),
                    `unit` VARCHAR(255) )
                    """)

#insert into schema
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('Date Time', 'Date and time of measurement', 'datetime')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('NOx', 'Concentration of oxides of nitrogen', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('NO2', 'Concentration of nitrogen dioxide', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('NO', 'Concentration of nitric oxide', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('SiteID', 'Site ID for the station', 'integer')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('PM10', 'Concentration of particulate matter <10 micron diameter', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('NVPM10', 'Concentration of non - volatile particulate matter <10 micron diameter', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('VPM10', 'Concentration of volatile particulate matter <10 micron diameter', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('NVPM2.5', 'Concentration of non volatile particulate matter <2.5 micron diameter', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('PM2.5', 'Concentration of particulate matter <2.5 micron diameter', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('VPM2.5', 'Concentration of volatile particulate matter <2.5 micron diameter', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('CO', 'Concentration of carbon monoxide', 'mg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('O3', 'Concentration of ozone', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('SO2', 'Concentration of sulphur dioxide', 'μg/m3')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('Temperature', 'Air temperature', '°C')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('RH', 'Relative Humidity', '%')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('Air Pressure', 'Air Pressure', 'mbar')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('Location', 'Text description of location', 'text')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('geo_point_2d', 'Latitude and longitude', 'geo point')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('DateStart', 'The date monitoring started', 'datetime')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('DateEnd', 'The date monitoring ended', 'datetime')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('Current', 'Is the monitor currently operating', 'text')""")
mycursor.execute("""INSERT INTO schema_table (`measure`, `desc`, `unit`) VALUES ('Instrument Type', 'Classification of the instrument', 'text')""")
                    

                    

mydb.commit()


#drops stations table if it already exists in the database
mycursor.execute("DROP TABLE IF EXISTS `stations`;") 


#create new table if it doesn't exist
mycursor.execute(""" CREATE TABLE stations (
                    `station_id` VARCHAR(255),
                    `location` VARCHAR(255),
                    `geo_point_2d` VARCHAR(255)
                    )""")

#insert values into station tables
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (188,'AURN Bristol Centre', '')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (203,'Brislington Depot', '51.4417471802,-2.55995583224')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (206,'Rupert Street', '51.4554331987,-2.59626237324')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (209,'IKEA M32', '')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (213,'Old Market', '51.4560189999,-2.58348949026')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (215,'Parson Street School', '51.432675707,-2.60495665673')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (228,'Temple Meads Station', '')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (270,'Wells Road', '51.4278638883,-2.56374153315')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (271,'Trailer Portway P&R', '')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (375,'Newfoundland Road Police Station', '51.4606738207,-2.58225341824')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (395,"Shiner's Garage", '51.4577930324,-2.56271419977')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (452,'AURN St Pauls', '51.4628294172,-2.58454081635')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (447,'Bath Road', '51.4425372726,-2.57137536073')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (459,'Cheltenham Road \ Station Road', '51.4689385901,-2.5927241667')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (463,'Fishponds Road', '51.4780449714,-2.53523027459')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (481,'CREATE Centre Roof', '51.447213417,-2.62247405516')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (500,'Temple Way', '51.4579497129,-2.58398909033')""")
mycursor.execute("""INSERT INTO stations (`station_id`, location, `geo_point_2d`) VALUES (501,'Colston Avenue', '51.4552693825,-2.59664882861')""")


mydb.commit()


