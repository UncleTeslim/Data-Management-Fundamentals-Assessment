# A reflective report on Data Management Fundamentals 2021 assignment



This assignment was issued to students on 11 November, 2021

This assignment would be submitted on 20 January, 2022

The assignment compises of every topic in this module that was taught during the weekly workshop sessions.



Task1a was easier with use of the *Pandas library*. The file data was cropped cwriting shorter codes with pandas library. 

Task1b was a bit more technical and it proved to be the first difficulty in the assignment. Dropping <u>SiteID</u> with null values was straightforward with the `.na` function of pandas library. However, I finally dropped mismatched <u>SiteID</u>. Later solved this by creating a dictionary that contains locations and respective <u>SIteID</u>. I then matched both and removed <u>SiteID</u> that doesnt match. The code is showed below

```
for row in range(0, len(records_from_jan_2010_filter)-1):

site_id = records_from_jan_2010_filter.iloc[row, 4]

location = records_from_jan_2010_filter.iloc[row, 17]

line_number = result_data.index[row]

if site_id not in site_id_location_comp_dict or site_id_location_comp_dict[site_id] != location:

print(f"line number: {line_number}, SiteID: {site_id}, Location: {location}")

index_rows.append(row)

result_data.drop(result_data.index[index_rows], inplace=True)
```



For Task2a, the module Leader, Prakash already gave a sample of expected ER diagram in one of the workshop sessions and all 
i did was recreate with MySQL Workbench. I then used the forward-engineer function of MySQL Workbench to generate the SQL schema. 
I learned how to draw ER models with MySQL Workbench while solving this task.



Arguably the most exhausting part of this assignment, I spent so much time on task3. I was able to connect  to MySQl 
easily and also create the tables, but I fount it difficult to poppulate them with values froom my **cleaned.csv** file. 
I solved this problem in two parts.
I also worked with Olabisi Ajilola (STudent ID 21047889) in solving this task

First, I converted my **cleaned.csv** into a dataframe, converted the dataframe into json format and 
then read into a list. I have included code junks of this solution

```
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
```

After creating my database tables, I inserted my rows from the list into the tables

```
for row in clean_csv_list:
    mycursor.execute(insert_data, ( row['Date Time'], row['NOx'], row['NO2'], row['NO'], row['SiteID'], 
                                row['PM10'], row['NVPM10'], row['VPM10'], row['NVPM2.5'], row['PM2.5'], 
                                row['VPM2.5'], row['CO'], row['O3'], row['SO2'], row['Temperature'], 
                                row['RH'], row['Air Pressure'], row['Location'], row['geo_point_2d'], row['DateStart'], 
                                row['DateEnd'], row['Current'], row['Instrument Type'])  ) 

```

I made use of the pandas, json and sql.connector libraries for this task



I chose MongoDB as the nosql database to model in task 5. I chose  MongoDb  because it is one of the most popular databases 
in the world and it is hightly available. It stores data in a json form and also fast because it can process large amount of unprocessed data. 


