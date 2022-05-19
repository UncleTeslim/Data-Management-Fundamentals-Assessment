#I imported pandas
import pandas as pd

air = pd.read_csv('bristol-air-quality-data.csv', delimiter= ';' )
# air.head()
# air.info()

# deleting any records before 00:00 1 Jan 2010 (1262304000)
cropped_air = air[air["Date Time"] > "2010-01-01 00:00"]
# cropped_air.info()
# print(cropped_air)

#Saving new cropped data
cropped_air.to_csv('crop.csv',index=False, sep = ';')