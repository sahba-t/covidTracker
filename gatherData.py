import pandas as pd
import os

import urllib.request, json
import sys

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
    for item in outputfiles:
        print(item)
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

        output = df.to_csv()

        item = item.split('.')[0] + '.csv'

        with open(item, 'w') as f:
            f.write(output)


def main():
    endpoints = ['https://covid19api.io/api/v1/JohnsHopkinsDataDailyReport', 
                'https://covid19api.io/api/v1/TestsInUS', 
                'https://covid19api.io/api/v1/FatalityRateByAge',
                'https://covid19api.io/api/v1/FatalityRateBySex',
                'https://covid19api.io/api/v1/FatalityRateByComorbidities',
                'https://covid19api.io/api/v1/USAMedicalAidDistribution',
                'https://covid19-server.chrismichael.now.sh/api/v1/AggregatedFacilityCapacityCounty',
                'https://covid19api.io/api/v1/UnitedStateCasesByStates']

    outputfiles = ['Data/JHU.json', 'Data/USTests.json', 'Data/FatalityAge.json', 'Data/FatalityBySex.json', 'Data/FatalityByComorbidities.json', 'Data/USMedAid.json', 'Data/FacilityCapacity.json', 'Data/USCases.json']

    collectData(endpoints, outputfiles)

    fixJSON(outputfiles)

main()
