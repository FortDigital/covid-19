#Ingest Data from https://github.com/open-covid-19 which consolidates multiple data sources
#This script should be used for initial ingest, Delta Script should be used for daily updates
#import csv
import requests
import itertools
import geohash
import pandas as pd
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
def add_tags(measurements_hash, response, key):
    row = response.query('key == "' + key + '"' )   
    if len(row.index) > 0:  
        for col, content in row.iteritems():
            if str(content.values[0]) != 'nan':
                measurements_hash['tags'][col] = content.values[0]      
    return measurements_hash
def add_fields(measurements_hash, response, key,date):
    row = response.query('key == "' + key + '" and date == "' + date + '"')   
    if len(row.index) > 0:  
        for col, content in row.iteritems():
            if col != "key" and col != "date":
                if str(content.values[0]) != 'nan':
                    measurements_hash['fields'][col] = content.values[0]      
    return measurements_hash 
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
indexfile = "https://open-covid-19.github.io/data/v2/index.csv"#All Tags
demographicsfile = "https://open-covid-19.github.io/data/v2/demographics.csv"#All Tags
economyfile = "https://open-covid-19.github.io/data/v2/economy.csv"#All Tags
epidemiologyfile = "https://open-covid-19.github.io/data/v2/epidemiology.csv" #Has Date
geographyfile = "https://open-covid-19.github.io/data/v2/geography.csv"#All Tags
healthfile = "https://open-covid-19.github.io/data/v2/health.csv"#All Tags
hospitalizationsfile = "https://open-covid-19.github.io/data/v2/hospitalizations.csv" #Has Date - All Fields (Apart from Date and key)
mobilityfile = "https://open-covid-19.github.io/data/v2/mobility.csv"#All Tags
oxfordfile = "https://open-covid-19.github.io/data/v2/oxford-government-response.csv" #Has Date - All Fields (Apart from Date and key)
weatherfile = "https://open-covid-19.github.io/data/v2/weather.csv" #Has Date - all fields (Apart from Date and key)
agefile = "https://open-covid-19.github.io/data/v2/by-age.csv" #Has Date - Quiet complicated, some Columns include bucket definitions - Skip for now
sexfile = "https://open-covid-19.github.io/data/v2/by-sex.csv" #Has Date - all fields (Apart from Date and key)
KeyHash = {}
measurements = []
measurements_hash = {}
measurecount = 0
#Iterate through each Source File and build hash table
####
#THIS NEEDS A FULL REWRITE, ONLY EPIDEMIOLOGYFILES HAVE DATETIMES, THE REST ARE kEYS TO LOOKUP AND ADD TAGS/FIELDS SO WILL NEED TO LOAD EACH KEY FILE INTO MEMORY AND LOOKUP BASED ON KEY FOR _EVERY_ DATE RECORD
###
response = pd.read_csv(epidemiologyfile)
indexresponse = pd.read_csv(indexfile)
geographyresponse = pd.read_csv(geographyfile)
demographicresponse = pd.read_csv(demographicsfile)
economyresponse = pd.read_csv(economyfile)
healthresponse = pd.read_csv(healthfile)
mobilityresponse = pd.read_csv(mobilityfile)
hospitalizationsresponse = pd.read_csv(hospitalizationsfile)
oxfordresponse = pd.read_csv(oxfordfile)
weatherresponse = pd.read_csv(weatherfile)
sexresponse = pd.read_csv(sexfile)
results = []
tag_array = ["key"]
field_array=["new_confirmed","new_deceased","new_recovered","new_tested","total_confirmed","total_deceased","total_tested"]
for label,record in response.iterrows():
    #Build lookup table for Converting Code to name for country and region
    datemdy = datetime.strptime(record['date'], '%Y-%m-%d').replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
    time_loc_hash = "{}:{}".format(datemdy, record['key'])       
    measurements_hash[time_loc_hash] = {'measurement': INFLUX_MEASUREMENT, 'tags': {}, 'fields': {}, 'time': int(datemdy) * 1000 * 1000 * 1000}
    for tag in tag_array:
        if record[tag] != "":
            measurements_hash[time_loc_hash]['tags'][tag] = record[tag]
    measurements_hash[time_loc_hash] = add_tags(measurements_hash[time_loc_hash],indexresponse,record['key'])
    measurements_hash[time_loc_hash] = add_tags(measurements_hash[time_loc_hash],geographyresponse,record['key'])
    measurements_hash[time_loc_hash] = add_tags(measurements_hash[time_loc_hash],demographicresponse,record['key'])
    measurements_hash[time_loc_hash] = add_tags(measurements_hash[time_loc_hash],economyresponse,record['key'])
    measurements_hash[time_loc_hash] = add_tags(measurements_hash[time_loc_hash],healthresponse,record['key'])
    measurements_hash[time_loc_hash] = add_tags(measurements_hash[time_loc_hash],mobilityresponse,record['key'])
    #
    measurements_hash[time_loc_hash] = add_fields(measurements_hash[time_loc_hash],hospitalizationsresponse,record['key'],record['date'])
    measurements_hash[time_loc_hash] = add_fields(measurements_hash[time_loc_hash],oxfordresponse,record['key'],record['date'])
    measurements_hash[time_loc_hash] = add_fields(measurements_hash[time_loc_hash],weatherresponse,record['key'],record['date'])
    measurements_hash[time_loc_hash] = add_fields(measurements_hash[time_loc_hash],sexresponse,record['key'],record['date'])
    for field in field_array:
        try:
            measurements_hash[time_loc_hash]['fields'][field] = int(record[field])
        except ValueError:
            measurements_hash[time_loc_hash]['fields'][field] = 0
    #Add tags and values from other files           
#Drop existing Measurement to ensure data consistency with Datasource being updated regularly
if INFLUX_DROPMEASUREMENT:
    client.drop_measurement(INFLUX_MEASUREMENT)               
#Iterate through Hash table and format for Influxdb Client
for m in measurements_hash:
    measurements.append(measurements_hash[m])
    measurecount+= 1
    if(measurecount == 1000):   
        client.write_points(measurements)
        measurements = []
        measurecount = 0
#Commit to Influxdb
if measurements:    
    client.write_points(measurements)        
