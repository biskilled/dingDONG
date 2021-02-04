"""
    Copyright (c) 2017-2021, BPMK LTD (BiSkilled) Tal Shany <tal@biSkilled.com>

    Download data
    original files downloaded from:
    http://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/CHDI/chsi_dataset.zip

"""

import requests
import os
from zipfile import ZipFile

def downloadFile (url, filePath, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(filePath, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
    print("FINISH TO DOWNLOAD FROM %s" % (url))

def unZip (filePath):
    head, tail = head, tail = os.path.split(filePath)
    with ZipFile(filePath, 'r') as zipObj:
        # Extract all the contents of zip file in current directory
        zipObj.extractall(head)
    print ("FINISH TO UNZIP INTO %s" %head)

URL = 'https://github.com/biskilled/dingDong/raw/master/samples/sampleHealthCare/csvData.zip'
FILE_PATH = "C:\\dingDONG\\csvData.zip"

downloadFile (url=URL, filePath=FILE_PATH, chunk_size=128)
unZip (filePath=FILE_PATH)


