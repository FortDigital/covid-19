# Covid-19

Grafana Visualisation and Python Data collection script for tracking covid-19 activity.

## Getting Started

DISCLAIMER: I will hold my hands up now and say im NOT a developer, this is my first foray into developing and releasing ANYTHING into the community so please be gentle with me :) That being said if you do have any pointers/improvements please feel free to share your views, Just dont be offended if i dont neccesarily act on them in this project!

### Screenshots

![Alt text](/Screenshots/Top.png?raw=true)
![Alt text](/Screenshots/Bottom.png?raw=true)

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

* JHU For gathering the data (https://github.com/CSSEGISandData/COVID-19)
* ExpDev07 for creating the API which was used to origionally access the data (https://github.com/ExpDev07/coronavirus-tracker-api)
