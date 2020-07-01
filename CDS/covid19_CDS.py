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
from zipfile import ZipFile

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
inputzip  = "https://coronadatascraper.com/timeseries-tidy.csv.zip"
inputfile = "timeseries-tidy.csv"
measurements = []
measurements_hash = {}
measurecount = 0
response = requests.get(inputzip)
if response.status_code != 200:
    print('Failed to get data:', response.status_code)
else:
    file = ZipFile(io.BytesIO(response.content))
    csvfile = file.open(inputfile).read().decode()
    wrapper = csv.DictReader(csvfile.strip().split('\n'))
    results = []
    for record in wrapper:
        field = record['type']
        today = datetime.today().replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
        country = record['country'].strip()
        #try:
            #Some Invalid Codes?
            #countryname = pycountry.countries.get(alpha_3=country).name
        #except:
            #countryname = ''
        city = record['city'].strip()
        state = record['state'].strip()
        county = record['county'].strip()
        level = record['level'].strip()
        location = record['name'].strip()
        aggregate = record['aggregate'].strip()
        #location_hash = "{} {} {}".format(country, state, county)
        datemdy = datetime.strptime(record['date'], '%Y-%m-%d').replace(hour=12, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
        time_loc_hash = "{}:{}".format(datemdy, location)       
        if time_loc_hash not in measurements_hash: 
            measurements_hash[time_loc_hash] = {'measurement': INFLUX_MEASUREMENT, 'tags': {}, 'fields': {}, 'time': int(datemdy) * 1000 * 1000 * 1000}
            measurements_hash[time_loc_hash]['tags']['location'] = location
            measurements_hash[time_loc_hash]['tags']['country'] = country
            measurements_hash[time_loc_hash]['tags']['state'] = state
            measurements_hash[time_loc_hash]['tags']['county'] = county
            measurements_hash[time_loc_hash]['tags']['aggregate'] = aggregate
            measurements_hash[time_loc_hash]['tags']['level'] = level
            measurements_hash[time_loc_hash]['tags']['city'] = city
            #measurements_hash[time_loc_hash]['tags']['countryname'] =countryname
            try:
                #Hard code for USA Geocache due to data feed issue
                #if country == 'United States' and county == '' and state == '':
                #    measurements_hash[time_loc_hash]['tags']['geohash'] = '9wy'
                #else:
                measurements_hash[time_loc_hash]['tags']['geohash'] = geohash.encode(float(record['lat']),float(record['long'])) # Generate Geohash for use with Grafana Plugin
            except:
                measurements_hash[time_loc_hash]['tags']['geohash'] = geohash.encode(float(0),float(0)) # Generates a dummy Geohash to satisfy Grafana
            try:
                measurements_hash[time_loc_hash]['fields']['population'] = int(record['population'])
            except ValueError:
                measurements_hash[time_loc_hash]['fields']['population'] = 0    
        try:
            measurements_hash[time_loc_hash]['fields'][field] = int(record['value']) 
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