# University of New Mexico
# Authors: Jered Dominguez-Trujillo and Sahba Tashakkori
# Date: May 5, 2020
# Description: Script to Obtain, Parse, and Organize COVID-19 Data for CS 564 Final Project


# Import Statements
import pandas as pd
import numpy as np
import os
import urllib.request, json
import sys
from datetime import date


# Function to Loop Through GET Requests and Save Raw JSON Data to Files
def collectData(endpoints, outputfiles):
    # John Hopkins Data Daily Report: https://covid19api.io/api/v1/JohnsHopkinsDataDailyReport
    # Tests in US: https://covid19api.io/api/v1/TestsInUS
    # Fatality Rate By Age: https://covid19api.io/api/v1/FatalityRateByAge - No longer using
    # Fatality Rate By Sex: https://covid19api.io/api/v1/FatalityRateBySex - No longer using
    # Fatality Rate By Co-morbidity: https://covid19api.io/api/v1/FatalityRateByComorbidities - No longer using
    # USA Medical Aid Distribution: https://covid19api.io/api/v1/USAMedicalAidDistribution
    # Aggregated Facility Capacity County: https://covid19-server.chrismichael.now.sh/api/v1/AggregatedFacilityCapacityCounty
    # United States Cases By State: https://covid19api.io/api/v1/UnitedStateCasesByStates

    ii = 0

    for url_endpoint in endpoints:
        # GET Request from Endpoint
        with urllib.request.urlopen(url_endpoint) as url:
            # Save JSON Data
            data = json.loads(url.read().decode())

            # Dump JSON Data into a file
            with open(outputfiles[ii], 'w') as f:
                json.dump(data, f)
        
        ii += 1


# Function to Get Rid of Weird Unicode Characters, To Un-Nest the Data from Original Organization, and To Convert to CSV File
def fixJSON(outputfiles):
    newfiles = []

    # Clean up JSON Data
    for item in outputfiles:
        df = pd.read_json(item)
        if 'JHU' in item:
            df = pd.DataFrame(df['data']['table'])

        elif 'USTESTS' in item:
            df = pd.DataFrame(df['tests']['table'])
            df['CDCLabs'] = df['CDCLabs'].str.encode('ascii', 'ignore').str.decode('ascii')
            df['USPublicHealthLabs'] = df['USPublicHealthLabs'].str.encode('ascii', 'ignore').str.decode('ascii')

        #elif 'Fatality' in item:
        #    df = pd.DataFrame(df['table'].tolist())

        elif 'USMEDAIDAGGREGATE' or 'USFACILITIES' or 'USCASEBYSTATE' in item:
            df = pd.DataFrame(df['data'][0]['table'])

        # Convert to CSV File
        item = item.split('.')[0] + '.csv'
        newfiles.append(item)

        with open(item, 'w') as f:
            f.write(df.to_csv(index=False))

    return newfiles


# Function to Convert Cleaned Up JSON Data to CSV Files That Correspond to our DB Tables
# Extract the desired fields from the jsons/ rename the columns
def prepDB(newfiles):
    # State Abbreviation to Name Dictionary for JHU Data
    State2Abb = {'Alabama': 'AL', 'Alaska': 'AK', 'American Samoa': 'AS', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'District of Columbia': 'DC', 'Florida': 'FL',
    'Georgia': 'GA', 'Guam': 'GU', 'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA',
    'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
    'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC',
    'North Dakota': 'ND', 'Northern Mariana Islands':'MP', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Puerto Rico': 'PR', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN',
    'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virgin Islands': 'VI', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'}
    
    #Abb2State = dict(map(reversed, State2Abb.items()))

    for item in newfiles:
        today = date.today()

        # Read original csv file into Dataframe
        with open(item, 'r') as f:
            df = pd.read_csv(item)

        # John Hopkins Data
        if 'JHU' in item:
            # Only get US data
            df = df[df['Country_Region'] == 'US']

            # Get city from combined_key of city, state, country
            df['Combined_Key'] = df['Combined_Key'].apply(lambda x: x.split(',')[0])

            # Re-assigning some things to be more readable and adding DateRecorded
            df = df.assign(DateRecorded=today.strftime("%m/%d/%Y"))
            df = df.rename(columns={'Lat': 'Latitude', 'Long_': 'Longitude', 'Country_Region': 'Country', 'Province_State': 'State', 'Combined_Key': 'City'})

            # Map State Names to State Abbreviations
            df['State'] = df['State'].map(State2Abb)

            # Drop un-needed columns
            df = df.drop(columns=['Last_Update'])

            # Re-Arranging Columns to Match DB Table
            # TABLE JHU (DateRecorded, Country, State, City, Confirmed, Deaths, Recovered, Active, Latitude, Longitude)
            cols = ['DateRecorded', 'Country', 'State', 'City', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Latitude', 'Longitude']
            dfJHU = df[cols]

            # Save CSV File to Different Folder
            item = 'DBInput/' + item.split('/')[1]
            with open(item, 'w') as f:
                f.write(dfJHU.to_csv(index=False))

        # US Cases by State Data - To Be Organized into 5 tables to minimize NULL entries
        if 'USCASEBYSTATE' in item:
            # Initial drop of irrelevant columns
            df = df.drop(columns=['positiveScore', 'negativeScore', 'negativeRegularScore', 'commercialScore', 'score', 'pending', 
            'lastUpdateEt', 'checkTimeEt', 'totalTestResults', 'posNeg', 'fips', 'dateModified', 'dateChecked'])
            
            # Re-assigning some things to be more readable and Adding DateRecorded
            df = df.assign(DateRecorded=today.strftime("%m/%d/%Y"))
            df = df.rename(columns={'state': 'State', 'positive': 'Positive', 'negative': 'Negative', 'death': 'Death', 'total': 'Total', 
            'dataQualityGrade': 'DataQualityGrade', 'dataGrade': 'DataGrade', 'hospitalizedCurrently': 'HospitalizedCurrently', 
            'hospitalizedCumulative': 'HospitalizedCumulative', 'inIcuCurrently': 'InICUCurrently', 'inIcuCumulative': 'InICUCumulative', 
            'onVentilatorCurrently': 'OnVentilatorCurrently', 'onVentilatorCumulative': 'OnVentilatorCumulative', 'recovered': 'Recovered'})

            # Re-Arranging Columns to Match DB Table
            # TABLE USCASEBYSTATE (DateRecorded, State, Positive, Negative, Death, Total, DataQualityGrade, DataGrade)
            cols = ['DateRecorded', 'State', 'Positive', 'Negative', 'Death', 'Total', 'DataQualityGrade', 'DataGrade']
            dfUSCase = df[cols]

            # Save CSV File to Different Folder
            item = 'DBInput/' + item.split('/')[1]
            with open(item, 'w') as f:
                f.write(dfUSCase.to_csv(index=False))

            # TABLE HOSPITALIZED (DateRecorded, State, HospitalizedCurrently, HospitalizedCumulative)
            dfHospitalized = df[['DateRecorded', 'State', 'HospitalizedCurrently', 'HospitalizedCumulative']]
            dfHospitalized = dfHospitalized.dropna(subset=['HospitalizedCurrently', 'HospitalizedCumulative'], how='all')

            # Save CSV File to Different Folder
            item = 'DBInput/HOSPITALIZED.csv'
            with open(item, 'w') as f:
                f.write(dfHospitalized.to_csv(index=False))

            # TABLE ICU (DateRecorded, State, InICUCurrently, InICUCumulative)
            dfICU = df[['DateRecorded', 'State', 'InICUCurrently', 'InICUCumulative']]
            dfICU = dfICU.dropna(subset=['InICUCurrently', 'InICUCumulative'], how='all')

            # Save CSV File to Different Folder
            item = 'DBInput/ICU.csv'
            with open(item, 'w') as f:
                f.write(dfICU.to_csv(index=False))

            # TABLE VENTILATOR (DateRecorded, State, OnVentilatorCurrently, OnVentilatorCumulative)
            dfVentilator = df[['DateRecorded', 'State', 'OnVentilatorCurrently', 'OnVentilatorCumulative']]
            dfVentilator = dfVentilator.dropna(subset=['OnVentilatorCurrently', 'OnVentilatorCumulative'], how='all')

            # Save CSV File to Different Folder
            item = 'DBInput/VENTILATOR.csv'
            with open(item, 'w') as f:
                f.write(dfVentilator.to_csv(index=False))

            # TABLE RECOVERED (DateRecorded, State, Recovered)
            dfRecovered = df[['DateRecorded', 'State', 'Recovered']]
            dfRecovered = dfRecovered.dropna(subset=['Recovered'], how='all')

            # Save CSV File to Different Folder
            item = 'DBInput/RECOVERED.csv'
            with open(item, 'w') as f:
                f.write(dfRecovered.to_csv(index=False))

        # US Facilities Data
        if 'USFACILITIES' in item:
            # Round numerical data to 3 decimal places
            df = df.round(3)

            # Re-assigning some things to be more readable and Adding DateRecorded
            df = df.assign(DateRecorded=today.strftime("%m/%d/%Y"))
            df = df.rename(columns={'Population_20_plus': 'Population20Plus', 'Population_65_plus': 'Population65Plus'})

            # Drop un-needed columns
            df.drop(columns=['fips_code', 'LicensedAllBedsPer1000Adults20_plus', 'LicensedAllBedsPer1000Elderly65_plus', 'StaffedAllBedsPer1000Adults20_plus', 
            'StaffedAllBedsPer1000Elderly65_plus', 'StaffedAllBedsPer1000People', 'StaffedICUBedsPer1000Adults20_plus', 'StaffedICUBedsPer1000Elderly65_plus', 'StaffedICUBedsPer1000People'])

            # Re-Arranging Columns to Match DB Table
            # TABLE USFACILITIES (DateRecorded, State, CountyName, Population, Population20Plus, Population65Plus, StaffedAllBed, StaffedICUBed, LicensedAllBeds)
            cols = ['DateRecorded', 'State', 'CountyName', 'Population', 'Population20Plus', 'Population65Plus', 'StaffedAllBeds', 'StaffedICUBeds', 'LicensedAllBeds']
            dfFacilities = df[cols]

            # Save CSV File to Different Folder
            item = 'DBInput/' + item.split('/')[1]
            with open(item, 'w') as f:
                f.write(dfFacilities.to_csv(index=False))

            # TABLE USALLBEDOCCUPANCY (DateRecorded, State, AllBedOccupancyRate)
            dfRecovered = df[['DateRecorded', 'State', 'AllBedOccupancyRate']]
            dfRecovered = dfRecovered.dropna(subset=['AllBedOccupancyRate'], how='all')

            # Save CSV File to Different Folder
            item = 'DBInput/USALLBEDOCCUPANCY.csv'
            with open(item, 'w') as f:
                f.write(dfRecovered.to_csv(index=False))

            # TABLE USICUBEDOCCUPANCY (DateRecorded, State, ICUBedOccupancyRate)
            dfRecovered = df[['DateRecorded', 'State', 'ICUBedOccupancyRate']]
            dfRecovered = dfRecovered.dropna(subset=['ICUBedOccupancyRate'], how='all')

            # Save CSV File to Different Folder
            item = 'DBInput/USICUBEDOCCUPANCY.csv'
            with open(item, 'w') as f:
                f.write(dfRecovered.to_csv(index=False))
        
        if 'USMEDAIDAGGREGATE' in item:
            # Split and Rename Columns
            df = df['state weight_lbs number_of_deliveries cost'.split()]
            df = df.rename(columns={'state': 'State', 'weight_lbs': 'Weight', 'cost': 'Cost', 'number_of_deliveries': 'Deliveries'})

            # Removing Dollar Sign
            df['Cost'] = df['Cost'].apply(lambda x: float(x.split('$')[1].replace(',', '')))

            # Grouping By State and Aggregating for Weight, Cost, and Deliveries
            df = df.groupby(['State']).sum()

            # Round Numerical Data to 3 Decimal Places
            df = df.round(3)

            # Re-assigning some things to be more readable and Adding DateRecorded
            df = df.assign(DateRecorded=today.strftime("%m/%d/%Y"))

            # After these Operations Pandas thinks State Column is Index - Fixed with Reset Index 
            df.reset_index(inplace=True)

            # Re-Arranging Columns to Match DB Table
            # TABLE USMEDAIDAGGREGATE (DateRecorded, State, Deliveries, Cost, Weight)
            cols = ['DateRecorded', 'State', 'Deliveries', 'Cost', 'Weight']
            dfMedAid = df[cols]

            # Save CSV File to Different Folder
            item = 'DBInput/' + item.split('/')[1]
            with open(item, 'w') as f:
                f.write(dfMedAid.to_csv(index=False))
    
        if 'USTESTS' in item:
            # Rename Columns
            df = df.rename(columns={'DateCollected': 'DateRecorded'})

            # Format Date Correctly for Oracle DMBS
            df['DateRecorded'] = df['DateRecorded'].apply(lambda x: x+"/2020")
            dfTests = df

            # TABLE USTEST(DataRecorded, CDCLabs, USPublicHealthLabs)
            # Save CSV File to Different Folder
            item = 'DBInput/' + item.split('/')[1]
            with open(item, 'w') as f:
                f.write(dfTests.to_csv(index=False))


# Main Routine to Gather Data with GET Requests and Prepare Data in CSV Format to be Fed into Oracle DBMS and our Database Design
def main():
    # Original 8 Endpoints Used - Decided to Remove the 3 Fatality Endpoints since they were not high quality, did not add a lot of information
    '''
    endpoints = ['https://covid19api.io/api/v1/JohnsHopkinsDataDailyReport', 
                'https://covid19api.io/api/v1/TestsInUS', 
                'https://covid19api.io/api/v1/FatalityRateByAge',
                'https://covid19api.io/api/v1/FatalityRateBySex',
                'https://covid19api.io/api/v1/FatalityRateByComorbidities',
                'https://covid19api.io/api/v1/USAMedicalAidDistribution',
                'https://covid19-server.chrismichael.now.sh/api/v1/AggregatedFacilityCapacityCounty',
                'https://covid19api.io/api/v1/UnitedStateCasesByStates']

    outputfiles = ['Data/JHU.json', 'Data/USTests.json', 'Data/FatalityAge.json', 'Data/FatalityBySex.json', 'Data/FatalityByComorbidities.json', 'Data/USMedAid.json', 'Data/FacilityCapacity.json', 'Data/USCases.json']
    '''

    # 5 Endpoints Used
    endpoints = ['https://covid19api.io/api/v1/JohnsHopkinsDataDailyReport', 
                'https://covid19api.io/api/v1/TestsInUS', 
                'https://covid19api.io/api/v1/USAMedicalAidDistribution',
                'https://covid19-server.chrismichael.now.sh/api/v1/AggregatedFacilityCapacityCounty',
                'https://covid19api.io/api/v1/UnitedStateCasesByStates']

    # Original Output Files
    outputfiles = ['Data/JHU.json', 'Data/USTESTS.json', 'Data/USMEDAIDAGGREGATE.json', 'Data/USFACILITIES.json', 'Data/USCASEBYSTATE.json']

    # Make Raw Data Directory
    os.system('mkdir -p Data')

    # Gather Data with GET Requests
    collectData(endpoints, outputfiles)

    # Clean Up Data
    newfiles = fixJSON(outputfiles)

    os.system("rm ./Data/*.json")
    os.system("mkdir -p DBInput")

    # Organize and Output Data for Oracle DBMS Insertion
    prepDB(newfiles)

# Run Main
if __name__ == "__main__":
    main()
