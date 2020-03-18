# Forked from FortDigital/covid-19

Added support for running FortDigital's script via Docker. The script runs every 30 minutes atm though source data is updated once a day. This can be easily changed in `run.py` in `/Docker`!

Image can be pulled from Docker Hub: `alexandzors/covid19:latest`

Composefile can be found in `/Docker`.

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

The following Variables can be set in the script to fit your Influxdb enviroment:

```
INFLUX_HOST = 'localhost'
INFLUX_DB = 'covid19' #Database must exist for Script to run
INFLUX_DBPORT =  8086
INFLUX_USER = '' #Leave blank if you are not using Auth
INFUX_PASS = '' #Leave blank if you are not using Auth
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

## Authors

* **Jamie Milton** - Fort Digital

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Cameron McCloskey for some pointers on Color scheme/layout
* JHU For gathering the data (https://github.com/CSSEGISandData/COVID-19)
* ExpDev07 for creating the API which was used to origionally access the data (https://github.com/ExpDev07/coronavirus-tracker-api)
