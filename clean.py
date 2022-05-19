# from numpy import info
import pandas as pd

records_from_jan_2010 = pd.read_csv('crop.csv', sep= ';')
    # drop records with null values for SiteID
records_from_jan_2010_filter = records_from_jan_2010.dropna(subset=["SiteID"])

    # convert site_id to int
records_from_jan_2010_filter = records_from_jan_2010_filter.astype({"SiteID": int})
result_data = records_from_jan_2010_filter

    # create SiteID / location dictionary
site_id_location_comp_dict = {188: "AURN Bristol Centre",
                                 203: "Brislington Depot",
                                 206: "Rupert Street",
                                 209: "IKEA M32",
                                 213: "Old Market",
                                 215: "Parson Street School",
                                 228: "Temple Meads Station",
                                 270: 'Wells Road',
                                 271: "Trailer Portway P&R",
                                 375: "Newfoundland Road Police Station",
                                 395: "Shiner's Garage",
                                 452: "AURN St Pauls",
                                 447: "Bath Road",
                                 459: "Cheltenham Road \ Station Road",
                                 463: "Fishponds Road",
                                 481: "CREATE Centre Roof",
                                 500: "Temple Way",
                                 501: "Colston Avenue"
                                 }
index_rows = []

    # drop mismatched or invalid SiteID
for row in range(0, len(records_from_jan_2010_filter)-1):
        site_id = records_from_jan_2010_filter.iloc[row, 4]
        location = records_from_jan_2010_filter.iloc[row, 17]
        line_number = result_data.index[row]
        if site_id not in site_id_location_comp_dict or site_id_location_comp_dict[site_id] != location:
            print(f"line number: {line_number}, SiteID: {site_id}, Location: {location}")
            index_rows.append(row)

result_data.drop(result_data.index[index_rows], inplace=True) 
#saving to csv 
result_data.to_csv('clean.csv', index=False, na_rep= 'NaN', sep= ',')
print(result_data.info())