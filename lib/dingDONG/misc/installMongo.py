import os
import wget
import shutil
from  zipfile import ZipFile
import subprocess

def p (s):
    print (s)

MONGO_PATH          = "C:\\MongoDB"
MONGO_DOWNLOAD_PREFIX = "https://fastdl.mongodb.org/win32"   # http://downloads.mongodb.org/win32/ /mongodb-win32-x86_64-2012plus-4.2.0.zip # mongodb-win32-x86_64-2008plus-2.4.9.zip
MONGO_DOWNLOAD_VERSION= "4.2.0"
MONGO_DOWNLOAD_FILE   = "mongodb-win32-x86_64-2012plus-%s" %(MONGO_DOWNLOAD_VERSION)

MONGO_DOWNLOAD_URL  = "%s/%s.zip" %(MONGO_DOWNLOAD_PREFIX, MONGO_DOWNLOAD_FILE)
MONGO_ZIP_FILE      = os.path.join (MONGO_PATH, "mongo.zip")
MONGO_UNZIP_FOLDER  = os.path.join (MONGO_PATH, MONGO_DOWNLOAD_FILE )
MONGO_UNZIP_NEW_NAME= 'mongoDb-%s' %(MONGO_DOWNLOAD_VERSION)
MONGO_CONFIG        = os.path.join (MONGO_PATH, 'mongod.cfg')
MONGO_LOG_FOLDER    = os.path.join (MONGO_PATH, 'log')
MONGO_DATA_FOLDER   = os.path.join(MONGO_PATH, 'data')
MONGO_DB_FOLDER     = os.path.join(MONGO_DATA_FOLDER, 'db')

import sys

def bar_custom(current, total, width=80):
    done = int(50 * current / total)
    sys.stdout.write('\r[{}{}]'.format('█' * done, '.' * (50 - done)))
    sys.stdout.flush()

    #print("\rDownloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total))

def installMongo (remove=True):
    if os.path.isdir(MONGO_PATH) and not remove:
        p("Seems you already installed MongoDB")
        return

    if not os.path.isdir(MONGO_PATH):       os.mkdir(MONGO_PATH)
    if not os.path.isdir(MONGO_LOG_FOLDER): os.mkdir(MONGO_LOG_FOLDER)
    if not os.path.isdir(MONGO_DATA_FOLDER): os.mkdir(MONGO_DATA_FOLDER)
    if not os.path.isdir(MONGO_DB_FOLDER): os.mkdir(MONGO_DB_FOLDER)

    configText = """systemLog:
    destination: file
    path: %s\mongod.log
storage:
    dbPath: %s
net:
    bindIp: 0.0.0.0
    port: 27017
    """ %(str(MONGO_LOG_FOLDER),str(MONGO_DB_FOLDER))

    if not os.path.isfile(MONGO_CONFIG):
        with open (MONGO_CONFIG, "w") as f:
            f.write(configText)

            #f.write("smallfiles=true \n" )
            #f.write("noprealloc=true \n" )

    try:
        if os.path.isfile(MONGO_ZIP_FILE):
            os.remove(MONGO_ZIP_FILE)
        p ("DOWNLOADING MONGODB FROM %s TO %s" %(MONGO_DOWNLOAD_URL,MONGO_ZIP_FILE))
        wget.download(MONGO_DOWNLOAD_URL, MONGO_ZIP_FILE, bar=bar_custom)

        with ZipFile(MONGO_ZIP_FILE, 'r') as zipObj:
            # Extract all the contents of zip file in different directory
            zipObj.extractall( MONGO_PATH )

        if os.path.isdir( os.path.join(MONGO_PATH,MONGO_UNZIP_NEW_NAME)):
            shutil.rmtree( os.path.join(MONGO_PATH,MONGO_UNZIP_NEW_NAME) )

        os.rename(MONGO_UNZIP_FOLDER,os.path.join(MONGO_PATH,MONGO_UNZIP_NEW_NAME))
        os.remove(MONGO_ZIP_FILE)

        mongodPath = os.path.join(MONGO_PATH,MONGO_UNZIP_NEW_NAME)
        mongodPath = os.path.join(mongodPath,'bin' )

        # C:\MongoDB\mongoDb-4.2.0\bin>mongod.exe --config c:\MongoDB\mongod.cfg --install
        subprocess.Popen([mongodPath + "\\" + "mongod.exe", '--config', MONGO_CONFIG, '--install'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    except Exception as e:
        p ("ERROR DOWNLOADING MONGODB, URL:%s, TARGET:%s" %(MONGO_DOWNLOAD_URL,MONGO_ZIP_FILE))
        p (e)

    # & $mongoDBPath\bin\mongod.exe --config $mongoDbConfigPath --install
    # & net start mongodb

def startMongoDB ():
    subprocess.Popen( ['net start', 'MongoDB'], shell=True, stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    # start mongodb


dic = [{}]


#installMongo ()
startMongoDB ()