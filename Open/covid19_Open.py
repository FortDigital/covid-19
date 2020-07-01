#Ingest Data from https://github.com/open-covid-19 which consolidates multiple data sources
#https://open-covid-19.github.io/data/data.csv - Main Datasource
#https://open-covid-19.github.io/data/data_forecast.csv - Forecast
#https://open-covid-19.github.io/data/data_categories.csv - Catagorised
import csv
import requests
import itertools
import geohash
#import pycountry
from datetime import datetime, tzinfo, timedelta
from influxdb import InfluxDBClient
class Zone(tzinfo):
    def __init__(self, offset, isdst, name):
        self.offset = offset
        self.isdst = isdst
        self.name = name

    def utcoffset(self, dt):
        return timedelta(hours=self.offset) + self.dst(dt)

    def dst(self, dt):
        return timedelta(hours=1) if self.isdst else timedelta(0)

    def tzname(self, dt):
        return self.name
INFLUX_HOST = 'localhost'
INFLUX_DB = 'covid19'
INFLUX_MEASUREMENT = 'covid19_Open'
INFLUX_DBPORT =  8086
INFLUX_USER = ''
INFUX_PASS = ''
INFLUX_DROPMEASUREMENT = True
client = InfluxDBClient(INFLUX_HOST, INFLUX_DBPORT,INFLUX_USER,INFUX_PASS, INFLUX_DB)
GMT = Zone(0, False, 'GMT')
#Direct Link to file
inputfile1 = "https://open-covid-19.github.io/data/data.csv"
inputfile2 = "https://open-covid-19.github.io/data/data_categories.csv"
inputfile3 = "https://open-covid-19.github.io/data/data_forecast.csv"
KeyHash = {}
measurements = []
measurements_hash = {}
measurecount = 0
SeverityTypes = ('NewMild','NewSevere','NewCritical','CurrentlyMild','CurrentlySevere','CurrentlyCritical')
#Iterate through each Source File and build hash table
response = requests.get(inputfile1)
if response.status_code != 200:
    print('Failed to get data:', response.status_code)
else:
    wrapper = csv.DictReader(response.text.strip().split('\n'))
    results = []
    for record in wrapper:
        countrycode = record['CountryCode'].strip()
        countryname = record['CountryName'].strip()
        regioncode = record['RegionCode'].strip()
        regionname = record['RegionName'].strip()
        location_hash = record['Key'].strip()
        #Build lookup table for Converting Code to name for country and region
        if location_hash not in KeyHash:
            KeyHash[location_hash] = [countryname,regionname]
        datemdy = datetime.strptime(record['Date'], '%Y-%m-%d').replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
        time_loc_hash = "{}:{}".format(datemdy, location_hash)       
        if time_loc_hash not in measurements_hash: #Feels like this is obselete?
            measurements_hash[time_loc_hash] = {'measurement': INFLUX_MEASUREMENT, 'tags': {}, 'fields': {}, 'time': int(datemdy) * 1000 * 1000 * 1000}
            measurements_hash[time_loc_hash]['tags']['location'] = location_hash
            measurements_hash[time_loc_hash]['tags']['countrycode'] = countrycode
            measurements_hash[time_loc_hash]['tags']['regioncode'] = regioncode
            measurements_hash[time_loc_hash]['tags']['regionname'] = regionname
            measurements_hash[time_loc_hash]['tags']['countryname'] =countryname
            try:
                measurements_hash[time_loc_hash]['tags']['geohash'] = geohash.encode(float(record['Latitude']),float(record['Longitude'])) # Generate Geohash for use with Grafana Plugin
            except:
                measurements_hash[time_loc_hash]['tags']['geohash'] = geohash.encode(float(0),float(0)) # Generates a dummy Geohash to satisfy Grafana
            try:
                measurements_hash[time_loc_hash]['fields']['population'] = int(record['Population'])
            except ValueError:
                measurements_hash[time_loc_hash]['fields']['population'] = 0         
            try:           
                measurements_hash[time_loc_hash]['fields']['confirmed'] = float(record['Confirmed']) #Data started coming in as floats
            except:
                print(location_hash + " is missing 'Confirmed' data " + record['Confirmed'])
            try:
                measurements_hash[time_loc_hash]['fields']['deaths'] = float(record['Deaths'])
            except:
                print(location_hash + " is missing 'Deaths' data " + record['Deaths'])
#Cycle through Second And third file adding to the existing Hash Table
response2 = requests.get(inputfile2)
if response2.status_code != 200:
    print('Failed to get data:', response2.status_code)
else:
    wrapper = csv.DictReader(response2.text.strip().split('\n'))
    results = []
    for record in wrapper:
        location_hash = record['Key'].strip()
        datemdy = datetime.strptime(record['Date'], '%Y-%m-%d').replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
        time_loc_hash = "{}:{}".format(datemdy, location_hash)       
        if time_loc_hash in measurements_hash:
            for SeverityType in SeverityTypes:
                try:
                    measurements_hash[time_loc_hash]['fields'][SeverityType] = int(record[SeverityType])
                except:
                    #print(location_hash + " is missing '"+SeverityType+"' data")  
                    break
#Cycle through Second And third file adding to the existing Hash Table
response3 = requests.get(inputfile3)
if response3.status_code != 200:
    print('Failed to get data:', response3.status_code)
else:
    wrapper = csv.DictReader(response3.text.strip().split('\n'))
    results = []
    for record in wrapper:
        location_hash = record['Key'].strip()
        if "_" in location_hash:
            countrycode = location_hash.split("_")[0]
            regioncode = location_hash.split("_")[1]
        else:
            countrycode = location_hash
            regioncode = ''    
        countryname =  KeyHash[location_hash][0]
        regionname  =  KeyHash[location_hash][1]
        datemdy = datetime.strptime(record['Date'], '%Y-%m-%d').replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
        time_loc_hash = "{}:{}".format(datemdy, location_hash)     
        if record['Estimated']:  
            if time_loc_hash not in measurements_hash:   
                measurements_hash[time_loc_hash] = {'measurement': INFLUX_MEASUREMENT, 'tags': {}, 'fields': {}, 'time': int(datemdy) * 1000 * 1000 * 1000}
                measurements_hash[time_loc_hash]['tags']['location'] = location_hash
                measurements_hash[time_loc_hash]['tags']['countrycode'] = countrycode
                measurements_hash[time_loc_hash]['tags']['regioncode'] = regioncode
                measurements_hash[time_loc_hash]['tags']['regionname'] = regionname
                measurements_hash[time_loc_hash]['tags']['countryname'] =countryname
                #print(measurements_hash[time_loc_hash])
            try:
                measurements_hash[time_loc_hash]['fields']['Estimated'] = float(record['Estimated'])
            except:
                print(location_hash + " is missing 'Estimated' data")  
#Drop existing Measurement to ensure data consistency with Datasource being updated regularly
    if INFLUX_DROPMEASUREMENT:
        client.drop_measurement(INFLUX_MEASUREMENT)               
    #Iterate through Hash table and format for Influxdb Client
    for m in measurements_hash:
        measurements.append(measurements_hash[m])
        measurecount+= 1
        if(measurecount == 1000):   
            client.write_points(measurements)
            #print(measurements)
            measurements = []
            measurecount = 0
    #Commit to Influxdb
    if measurements:    
        client.write_points(measurements)        
        #print(CountryHash)
        #print(measurements)