# Authors: Jered Dominguez-Trujillo and Sahba Tashakkori
# Date: May 5, 2020
# Description: Script to Obtain, Parse, and Organize COVID-19 Data for CS 564 Final Project

import pandas as pd
import os
import urllib.request, json
import sys

from datetime import date

def collectData(endpoints, outputfiles):
    # John Hopkins Data Daily Report: https://covid19api.io/api/v1/JohnsHopkinsDataDailyReport
    # Tests in US: https://covid19api.io/api/v1/TestsInUS
    # Fatality Rate By Age: https://covid19api.io/api/v1/FatalityRateByAge
    # Fatality Rate By Sex: https://covid19api.io/api/v1/FatalityRateBySex
    # Fatality Rate By Co-morbidity: https://covid19api.io/api/v1/FatalityRateByComorbidities
    # USA Medical Aid Distribution: https://covid19api.io/api/v1/USAMedicalAidDistribution
    # Aggregated Facility Capacity County: https://covid19-server.chrismichael.now.sh/api/v1/AggregatedFacilityCapacityCounty
    # United States Cases By State: https://covid19api.io/api/v1/UnitedStateCasesByStates

    ii = 0

    for url_endpoint in endpoints:
        with urllib.request.urlopen(url_endpoint) as url:
            #if you want to play with a json object
            data = json.loads(url.read().decode())
            # print(data)

            with open(outputfiles[ii], 'w') as f:
                json.dump(data, f)
        
        ii += 1

def fixJSON(outputfiles):
    newfiles = []

    for item in outputfiles:
        df = pd.read_json(item)
        if 'JHU' in item:
            df = pd.DataFrame(df['data']['table'])
        elif 'USTests' in item:
            df = pd.DataFrame(df['tests']['table'])
            df['CDCLabs'] = df['CDCLabs'].str.encode('ascii', 'ignore').str.decode('ascii')
            df['USPublicHealthLabs'] = df['USPublicHealthLabs'].str.encode('ascii', 'ignore').str.decode('ascii')
        elif 'Fatality' in item:
            df = pd.DataFrame(df['table'].tolist())
        elif 'USMedAid' or 'FacilityCapacity' or 'USCases' in item:
            df = pd.DataFrame(df['data'][0]['table'])

        output = df.to_csv(index=False)

        item = item.split('.')[0] + '.csv'

        newfiles.append(item)

        with open(item, 'w') as f:
            f.write(output)

    return newfiles


def prepDB(newfiles):
    for item in newfiles:
        today = date.today()

        with open(item, 'r') as f:
            df = pd.read_csv(item)

        
        #extract the desired fields from the jsons/ rename the columns
        
        if 'JHU' in item:
            df['Combined_Key'] = df['Combined_Key'].apply(lambda x: x.split(',')[0])
            df = df.assign(Latitude=df['Lat'])
            df = df.assign(Longitude=df['Long_'])
            df = df.assign(City=df['Combined_Key'])
            df = df.assign(Date=today.strftime("%m/%d/%Y"))

            df = df.drop(columns=['Lat', 'Long_', 'Last_Update', 'Combined_Key'])

        if 'USCases' in item:
            df = df.drop(columns=['positiveScore', 'negativeScore', 'negativeRegularScore', 'commercialScore', 'score', 'pending', 'lastUpdateEt', 'checkTimeEt', 'totalTestResults', 'posNeg', 'fips', 'dateModified', 'dateChecked'])
            df = df.assign(Date=today.strftime("%m/%d/%Y"))

        if 'Facility' in item:
            df = df.round(3)
            df = df.drop(columns=['fips_code'])
            df = df.assign(Date=today.strftime("%m/%d/%Y"))

        
        if 'USMedAid' in item:
            df = df['state weight_lbs number_of_deliveries cost'.split()]
            df = df.rename(columns={'weight_lbs':'Weight', 'cost': 'Cost'})
            #getting rid of the dolloar sign
            df['Cost'] = df['Cost'].apply(lambda x: float(x.split('$')[1].replace(',', '')))
            df = df.groupby(['state']).sum()
            df = df.round(3)
            df = df.assign(Date=today.strftime("%m/%d/%Y"))
            #after these operations pandas thinks state 
            df.reset_index(inplace=True)
    
        if 'USTests' in item:
            df['DateCollected'] = df['DateCollected'].apply(lambda x: x+"/2020")
            
        
        item = 'DBInput/' + item.split('/')[1]

        with open(item, 'w') as f:
            f.write(df.to_csv(index=False))



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

    outputfiles = ['Data/JHU.json', 'Data/USTests.json', 'Data/USMedAid.json', 'Data/FacilityCapacity.json', 'Data/USCases.json']

    os.system('mkdir -p Data')

    collectData(endpoints, outputfiles)

    newfiles = fixJSON(outputfiles)

    os.system("rm ./Data/*.json")

    os.system("mkdir -p DBInput")

    prepDB(newfiles)

if __name__ == "__main__":
    main()
