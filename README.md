# Covid-19

Grafana Visualisation and Python Data collection script for tracking covid-19 activity.

## Getting Started

### Screenshots

![Alt text](/Screenshots/Full.png?raw=true)


### Prerequisites

```
Grafana
Influxdb
Python 3.x
```

### Installing

To run the Python Script you will need the following modules:

```
csv
requests
itertools
geohash
datetime 
influxdb 
```

Or

```
pip install -r requirements.txt
```

The following Variables can be set in the script to fit your Influxdb enviroment. Only edit the second half of the variable.

Example: os.getenv('DBHOST', 'CHANGE THIS ONE ONLY')

```
INFLUX_HOST = os.getenv('DBHOST', 'localhost') #InfluxDB Host
INFLUX_DB = os.getenv('DB', 'covid19') #InfluxDB DB
INFLUX_DBPORT =  os.getenv('DBPORT', 8086) #InfluxDB Port
INFLUX_USER = os.getenv('DBUSER', '') #InfluxDB User. Leave blank if you do not use auth.
INFUX_PASS = os.getenv('DBPASS', '') #InfluxDB Password. Leave blank if you do not use auth.
```

The Script can be run from a scheduled task, the target repository is updated every evening:

```
https://github.com/CSSEGISandData/COVID-19
```

Grafana Plugins Required:

```
https://grafana.com/grafana/plugins/grafana-worldmap-panel
```

Import Grafana.json into Grafana:

```
https://grafana.com/docs/grafana/latest/reference/export_import/#importing-a-dashboard
```
# Docker

Image is located @ https://hub.docker.com/alexandzors/covid19

It can be run with the following paramters:

```
docker run -d --name=covid19 -e "DBHOST=localhost" -e "DB=covid19" -e "DBPORT=8086" -e "DBUSER=user" -e "DBPASS=password"`
```

A compose file is provided in `/Docker`.

The script runs every 4 hours via a cron job in the container. Cron settings can be changed via `/Docker/run.py` and then built locally using the Dockerfile in `/Docker/`.

**Note**: You will need to change the first build stage if you plan on building locally.

## Authors

* **Jamie Milton** - Fort Digital

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Cameron McCloskey for some pointers on Color scheme/layout
* JHU For gathering the data (https://github.com/CSSEGISandData/COVID-19)
* ExpDev07 for creating the API which was used to origionally access the data (https://github.com/ExpDev07/coronavirus-tracker-api)
