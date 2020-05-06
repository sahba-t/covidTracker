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

        elif 'USMEDAID' or 'USFACILITIES' or 'USCASEBYSTATE' in item:
            df = pd.DataFrame(df['data'][0]['table'])

        # Convert to CSV File
        item = item.split('.')[0] + '.csv'
        newfiles.append(item)

        with open(item, 'w') as f:
            f.write(df.to_csv(index=False))

    return newfiles


# Function to Convert Cleaned Up JSON Data to CSV Files That Correspond to our DB Tables
def prepDB(newfiles):
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

            # Re-assigning some things to be more readable
            df = df.assign(Latitude=df['Lat'])
            df = df.assign(Longitude=df['Long_'])
            df = df.assign(Country=df['Country_Region'])
            df = df.assign(State=df['Province_State'])
            df = df.assign(City=df['Combined_Key'])
            df = df.assign(DateRecorded=today.strftime("%m/%d/%Y"))

            # Drop duplicate and un-needed columns
            df = df.drop(columns=['Lat', 'Long_', 'Last_Update', 'Country_Region', 'Province_State', 'Combined_Key'])

            # Re-Arranging Columns to Match DB Table
            # TABLE JHU (DateRecorded, Country, State, City, Confirmed, Deaths, Recovered, Active, Latitude, Longitude)
            cols = ['DateRecorded', 'Country', 'State', 'City', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Latitude', 'Longitude']
            df = df[cols]

            # Save CSV File to Different Folder
            item = 'DBInput/' + item.split('/')[1]
            with open(item, 'w') as f:
                f.write(df.to_csv(index=False))

        # US Cases by State Data - To Be Organized into 5 tables to minimize NULL entries
        if 'USCASEBYSTATE' in item:
            # Initial drop of irrelevant columns
            df = df.drop(columns=['positiveScore', 'negativeScore', 'negativeRegularScore', 'commercialScore', 'score', 'pending', 
            'lastUpdateEt', 'checkTimeEt', 'totalTestResults', 'posNeg', 'fips', 'dateModified', 'dateChecked'])
            
            # Re-assigning some things to be more readable
            df = df.assign(DateRecorded=today.strftime("%m/%d/%Y"))
            df = df.assign(State=df['state'])
            df = df.assign(Positive=df['positive'])
            df = df.assign(Negative=df['negative'])
            df = df.assign(Death=df['death'])
            df = df.assign(Total=df['total'])
            df = df.assign(DataQualityGrade=df['dataQualityGrade'])
            df = df.assign(DataGrade=df['dataGrade'])
            df = df.assign(HospitalizedCurrently=df['hospitalizedCurrently'])
            df = df.assign(HospitalizedCumulative=df['hospitalizedCumulative'])
            df = df.assign(InICUCurrently=df['inIcuCurrently'])
            df = df.assign(InICUCumulative=df['inIcuCumulative'])
            df = df.assign(OnVentilatorCurrently=df['onVentilatorCurrently'])
            df = df.assign(OnVentilatorCumulative=df['onVentilatorCumulative'])
            df = df.assign(Recovered=df['recovered'])

            # Drop duplicate and un-needed columns
            df.drop(columns=['state', 'positive', 'negative', 'death', 'total', 'dataQualityGrade', 'dataGrade', 
            'hospitalizedCurrently', 'hospitalizedCumulative', 'inIcuCurrently', 'inIcuCumulative', 'onVentilatorCurrently', 'onVentilatorCumulative', 'recovered'])

            # Re-Arranging Columns to Match DB Table
            # TABLE USCASEBYSTATE (DateRecorded, State, Positive, Negative, Death, Total, DataQualityGrade, DataGrade)
            cols = ['DateRecorded', 'State', 'Positive', 'Negative', 'Death', 'Total', 'DataQualityGrade', 'DataGrade']
            dfUSCase = df[cols]

            # Save CSV File to Different Folder
            item = 'DBInput/' + item.split('/')[1]
            with open(item, 'w') as f:
                f.write(dfUSCase.to_csv(index=False))

            print(df.head())

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

            # Re-assigning some things to be more readable
            df = df.assign(DateRecorded=today.strftime("%m/%d/%Y"))
            df = df.assign(Population20Plus=df['Population_20_plus'])
            df = df.assign(Population65Plus=df['Population_65_plus'])

            # Drop duplicate and un-needed columns
            df.drop(columns=['fips_code', 'Population_20_plus', 'Population_65_plus', 
            'LicensedAllBedsPer1000Adults20_plus', 'LicensedAllBedsPer1000Elderly65_plus', 'StaffedAllBedsPer1000Adults20_plus', 
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




def main():
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

    endpoints = ['https://covid19api.io/api/v1/JohnsHopkinsDataDailyReport', 
                'https://covid19api.io/api/v1/TestsInUS', 
                'https://covid19api.io/api/v1/USAMedicalAidDistribution',
                'https://covid19-server.chrismichael.now.sh/api/v1/AggregatedFacilityCapacityCounty',
                'https://covid19api.io/api/v1/UnitedStateCasesByStates']

    outputfiles = ['Data/JHU.json', 'Data/USTESTS.json', 'Data/USMEDAID.json', 'Data/USFACILITIES.json', 'Data/USCASEBYSTATE.json']

    os.system('mkdir -p Data')

    collectData(endpoints, outputfiles)

    newfiles = fixJSON(outputfiles)

    os.system("rm ./Data/*.json")

    os.system("mkdir -p DBInput")

    prepDB(newfiles)

main()
