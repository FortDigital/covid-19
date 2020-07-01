#Ingest Data from https://coronadatascraper.com/ which consolidates multiple data sources
#https://coronadatascraper.com/timeseries.csv
#https://coronadatascraper.com/timeseries-tidy.csv # this seems to be the best file to work with - Now being presented as a zip file, and takes AGES to load and parse... Probably time to switch to a different file
#https://coronadatascraper.com/timeseries-jhu.csv
import csv
import requests
import itertools
import geohash
#eeimport pycountry
import io
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
INFLUX_MEASUREMENT = 'covid19_CDS'
INFLUX_DBPORT =  8086
INFLUX_USER = ''
INFUX_PASS = ''
INFLUX_DROPMEASUREMENT = True
client = InfluxDBClient(INFLUX_HOST, INFLUX_DBPORT,INFLUX_USER,INFUX_PASS, INFLUX_DB)
GMT = Zone(0, False, 'GMT')
#Direct Links to file from coronadatascraper
inputfile  = "https://coronadatascraper.com/timeseries.csv"
tag_array = ["name","level","county","state","country","lat","long","aggregate"]
field_array = ["population","cases","deaths","recovered","active","tested","hospitalized","hospitalized_current","discharged","icu","icu_current","growthFactor"]
measurements = []
measurements_hash = {}
measurecount = 0
response = requests.get(inputfile)
if response.status_code != 200:
    print('Failed to get data:', response.status_code)
else:
    wrapper = csv.DictReader(response.text.strip().split('\n'))
    results = []
    for record in wrapper:
        today = datetime.today().replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
        datemdy = datetime.strptime(record['date'], '%Y-%m-%d').replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
        time_loc_hash = "{}:{}".format(datemdy, record['name'].strip())       
        if time_loc_hash not in measurements_hash: 
            measurements_hash[time_loc_hash] = {'measurement': INFLUX_MEASUREMENT, 'tags': {}, 'fields': {}, 'time': int(datemdy) * 1000 * 1000 * 1000}
            for tag in tag_array:
                if record[tag].strip() != "":
                    measurements_hash[time_loc_hash]['tags'][tag] = record[tag].strip()       
            try:
                measurements_hash[time_loc_hash]['tags']['geohash'] = geohash.encode(float(record['lat']),float(record['long'])) # Generate Geohash for use with Grafana Plugin
            except:
                measurements_hash[time_loc_hash]['tags']['geohash'] = geohash.encode(float(0),float(0)) # Generates a dummy Geohash to satisfy Grafana
        for field in field_array:
            try:
                measurements_hash[time_loc_hash]['fields'][field] = int(record[field])
            except ValueError:
                measurements_hash[time_loc_hash]['fields'][field] = 0     
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
    #print(measurements)