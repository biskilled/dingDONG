import os
try:
    from pip import main as pipmain
except:
    from pip._internal import main as pipmain

try:
    import urllib2 as urlReq
except:
    import urllib.request as urlReq

def internet_on(host='http://google.com'):
    try:
        urlReq.urlopen(host, timeout=1)
        return True
    except urlReq.URLError as err:
        return False

def getPropertiesFile ():
    rootPath = os.path.dirname ( os.path.dirname (os.path.dirname (os.path.dirname(__file__))))
    reqFile = os.path.join(rootPath, 'requirements.txt')

    if os.path.isfile(reqFile):
        print ("requirements file found !")
        return reqFile
    else:
        print ("%s if not found ! " %reqFile)


def installModule (localFolder=None):
    reqFile = getPropertiesFile ()

    if not reqFile:
        return

    if not localFolder and internet_on():
        print ("Dowloading PIP packages from the web")
        pipmain(['install', '-r', reqFile])

    elif localFolder:
        if os.path.isdir(localFolder):
            pipmain(['install', '-r', reqFile, '--no-index', '--find-links', 'file:'+localFolder])
        else:
            print ('%s IS NOT FOLDER !!', localFolder)
    else:
        print ("NO INTERNET CONNECTION OR NO VALID LOCAL FOLDER ")
    #pipmain(['install', moduleName])

def downloadModules (localFolder):
    reqFile = getPropertiesFile()

    if not reqFile:
        return

    if not os.path.isdir(localFolder):
        print ("path %s is not local folder to download PIP packages.. " %localFolder)
        return

    if not internet_on():
        print ("MUST HAVE INTERENET CONNECTION TO START DOWNLOAD ! ")
        return

    print ("DOWNLOADING ....")
    pipmain(['download', '-r', reqFile, '-d', localFolder])


localFolder = 'C:\\Users\\tal.shani\\Documents\\biSkilled\\Zoho WorkDrive (biskilled)\\My Folders\\work\\sourceControl\\gitHub\\dingDONG2\\pipWhl'
#downloadModules(localFolder=localFolder)
#installModule (localFolder=localFolder)
