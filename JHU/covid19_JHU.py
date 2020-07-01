#Ingest Data from JHU CCSE Covid19 Repository https://github.com/CSSEGISandData/COVID-19
#Confirmed
#https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv
#Deaths
#https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv
#Recovered
#https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv
import csv
import requests
import itertools
import geohash
from geonamescache.mappers import country
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
INFLUX_MEASUREMENT = 'covid19_JHU'
INFLUX_DBPORT =  8086
INFLUX_USER = ''
INFUX_PASS = ''
INFLUX_DROPMEASUREMENT = True
client = InfluxDBClient(INFLUX_HOST, INFLUX_DBPORT,INFLUX_USER,INFUX_PASS, INFLUX_DB)
GMT = Zone(0, False, 'GMT')
mapperPop = country(from_key='name', to_key='population')
mapperISO3 = country(from_key='name', to_key='iso3')
#Direct Links to the 3 CSV Files maintained by JHU CCSE
inputfiles = {"confirmed":"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv","deaths":"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv","recovered":"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"}
measurements = []
measurements_hash = {}
#Iterate through each Source File and build hash table
for i in sorted(inputfiles.keys()):
    field = i
    url = inputfiles[i]
    response = requests.get(url)
    if response.status_code != 200:
        print('Failed to get data:', response.status_code)
    else:
        wrapper = csv.DictReader(response.text.strip().split('\n'))
        results = []
        for record in wrapper:
            today = datetime.today().replace(hour=22, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
            country = record['Country/Region'].strip()
            province = record['Province/State'].strip()
            location_hash = "{} {}".format(country, province)
            datekeys=len(record)-4
            for k in sorted(record.keys())[:datekeys]:    
                datemdy = datetime.strptime(k, '%m/%d/%y').replace(hour=22, minute=0, second=0, microsecond=0).replace(tzinfo=GMT).timestamp()
                time_loc_hash = "{}:{}".format(datemdy, location_hash)        
                if time_loc_hash not in measurements_hash: 
                    measurements_hash[time_loc_hash] = {'measurement': INFLUX_MEASUREMENT, 'tags': {}, 'fields': {}, 'time': int(datemdy) * 1000 * 1000 * 1000}
                    measurements_hash[time_loc_hash]['tags']['location'] = location_hash
                    measurements_hash[time_loc_hash]['tags']['country'] = country
                    measurements_hash[time_loc_hash]['tags']['province'] = province
                    measurements_hash[time_loc_hash]['tags']['geohash'] = geohash.encode(float(record['Lat']),float(record['Long'])) # Generate Geohash for use with Grafana Plugin 
                    #Population for Countries only
                    if province != "":
                        #Get Population of province if i can work out how
                        #May be better off just dropping Province and Totalling the Values into Country
                        measurements_hash[time_loc_hash]['fields']['population'] = 0
                        measurements_hash[time_loc_hash]['tags']['ISO3Code'] = ""
                    else:
                        measurements_hash[time_loc_hash]['fields']['population'] = mapperPop(country)
                        measurements_hash[time_loc_hash]['tags']['ISO3Code'] = mapperISO3(country)                      
                try:
                    measurements_hash[time_loc_hash]['fields'][field] = int(record[k]) 
                except ValueError:
                    measurements_hash[time_loc_hash]['fields'][field] = 0    
 
#Drop existing Measurement to ensure data consistency with Datasource being updated regularly
if INFLUX_DROPMEASUREMENT:
    client.drop_measurement(INFLUX_MEASUREMENT)
#Iterate through Hash table and format for Influxdb Client
for m in measurements_hash:
    measurements.append(measurements_hash[m])   
#Commit to Influxdb
if measurements:    
    client.write_points(measurements)