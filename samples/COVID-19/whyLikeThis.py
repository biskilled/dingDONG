# (c) 2017-2020, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of dingDong
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any late r version.
#
# dingDong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDong.  If not, see <http://www.gnu.org/licenses/>.
import os
import requests
import logging
from dingDONG import dingDONG
from dingDONG import Config

## 0. Global
DATA_FOLDER = 'C:\\dingDONG\\COVID19'
URL_EUROPA  = 'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv'
URL_UK      = 'https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv'
URL_WORLDDATA='https://covid.ourworldindata.org/data/owid-covid-data.csv'

Config.LOGS_DEBUG = logging.DEBUG
Config.CONNECTIONS = {
    'folder': DATA_FOLDER,
    'sqlite': {"url":'%s\\%s' %(DATA_FOLDER,"sqlLiteDB.db") }
    }


## 1. Downloading CSV data
# downloading corona csv files from 2 location
def downloadCSV (url, fold, fileName):
    fileName = '%s.csv' if '.csv' not in fileName else fileName
    print ("START DOWNLOAD FROM %s" %(url))
    response = requests.get(url,  verify=False)
    with open(os.path.join(fold, fileName), 'wb') as f:
        f.write(response.content)
    print("FINISH CREATED FILE %s" % (os.path.join(fold, fileName)))

downloadCSV (url=URL_EUROPA, fold=DATA_FOLDER, fileName='euro.csv')
downloadCSV (url=URL_UK, fold=DATA_FOLDER, fileName='uk.csv')
downloadCSV (url=URL_WORLDDATA, fold=DATA_FOLDER, fileName='world.csv')

## 2. load data into DB
nodesToLoad = [
    {"source": {"type":"folder", "filter":"csv"},
     "target": {"type":"sqlite" }
     }
]

dd = dingDONG(dicObj=nodesToLoad, filePath=None, dirData=None,
              includeFiles=None,notIncludeFiles=None,connDict=None, processes=1)

dd.ding()
dd.dong()



