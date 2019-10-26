# Copyright (c) 2017-2019, BPMK LTD (BiSkilled) Tal Shany <tal.shany@biSkilled.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
#
# This file is part of dingDong
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# dingDong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDong.  If not, see <http://www.gnu.org/licenses/>.

try:
    import queue
except ImportError:
    import Queue as queue

import traceback
import sys
from threading import Thread

from dingDong.bl.ddManager import ddManager

from dingDong.config            import config
from dingDong.misc.logger       import p, LOGGER_OBJECT
from dingDong.bl.jsonParser     import jsonParser
from dingDong.misc.enumsJson    import eJson
from dingDong.conn.baseConnectorManager   import mngConnectors as conn

## Execters
from dingDong.executers.executeSql import execQuery
from dingDong.executers.executeAddMsg import executeAddMsg
from dingDong.executers.executeMicrosoftOLAP import OLAP_Process
from dingDong.executers.executeVersionsGit import dbVersions


class dingDong:
    def __init__ (self,  dicObj=None, filePath=None,
                dirData=None, includeFiles=None, notIncludeFiles=None,
                dirLogs=None,connDict=None, processes=None, sqlFolder=None):

        self._dicObj        = dicObj
        self._filePath      = filePath
        self._dirData       = dirData
        self._includeFiles  = includeFiles
        self._notIncludeFiles=notIncludeFiles
        self._dirLogs       = None
        self.propcesses     = config.DONG_MAX_PARALLEL_THREADS
        self.sqlFolder      = config.SQL_FOLDER_DIR
        self.connDict       = connDict

        self.Set(dicObj=self._dicObj, filePath=self._filePath,
                dirData=self._dirData, includeFiles=self._includeFiles,
                notIncludeFiles=self._notIncludeFiles,dirLogs=dirLogs,
                connDict=self.connDict, processes=processes, sqlFolder=sqlFolder)

        self.msg = executeAddMsg()

        ## Set version location
        self.versionManager = dbVersions (folder=config.VERSION_DIR, vFileName=config.VERSION_FILE, vFileData=config.VERSION_FILE_DATA, url=config.VERSION_DB_URL, conn=config.VERSION_DB_CONN, tbl=config.VERSION_DB_TABLE)

        ## Defualt properties
        self.Config         = config

    def Set (self, dicObj=None, filePath=None,
                dirData=None, includeFiles=None, notIncludeFiles=None,
                dirLogs=None,connDict=None, processes=None, sqlFolder=None):

        self.sqlFolder = sqlFolder if sqlFolder else self.sqlFolder
        self._dicObj    = dicObj
        self._filePath  = filePath
        self._dirData   = dirData
        self._includeFiles  = includeFiles
        self._notIncludeFiles=notIncludeFiles
        self.connDict = connDict if connDict else self.connDict

        if dicObj or filePath or dirData or includeFiles or notIncludeFiles \
                or connDict or sqlFolder:
            self.jsonParser = jsonParser(dicObj=self._dicObj, filePath=self._filePath,
                                         dirData=self._dirData, includeFiles=self._includeFiles, notIncludeFiles=notIncludeFiles,
                                         connDict=self.connDict, sqlFolder=self.sqlFolder)

            self.connDict = self.jsonParser.connDict

        self.propcesses = processes if processes else self.propcesses

        self._dirLogs = dirLogs if dirLogs else config.LOGS_DIR
        if self._dirLogs:
            LOGGER_OBJECT.setLogsFiles(logDir=self._dirLogs)

    def ding (self, destList=None, jsName=None, jsonNodes=None):
        p('STARTING TO MODEL DATA STRUCURE >>>>>' , "i")
        allNodes = self.__getNodes(destList=destList, jsName=jsName, jsonNodes=jsonNodes)

        ## ALL Files
        for jsName, jsonNodes in allNodes:
            ## ALL On all nodes
            for jMap in jsonNodes:
                dingObject = ddManager(node=jMap, connDict=self.connDict, versionManager=self.versionManager)
                dingObject.ding()
                self.msg.addStateCnt()

        p('FINSHED TO MODEL DATA STRUCURE >>>>>', "i")
        p('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', "ii")

    ## There is parrallel processing option
    def dong (self, destList=None, jsName=None, jsonNodes=None):
        p('STARTING TO EXTRACT AND LOAD >>>>>', "i")
        allNodes = self.__getNodes(destList=destList, jsName=jsName, jsonNodes=jsonNodes)
        processList = []

        for jsName, jsonNodes in allNodes:
            procTotal = len(jsonNodes)
            for procNum, jMap in  enumerate (jsonNodes):
                processList.append((jMap, procNum, procTotal))
                self.msg.addStateCnt()


        numOfProcesses = min (len(processList), self.propcesses)

        if numOfProcesses==1:
            for proc in processList:
                dingObject = ddManager(node=proc[0], connDict=self.connDict)
                dingObject.dong()

        elif numOfProcesses > 1:
            q = queue.Queue(maxsize=0)
            for i, processParams in enumerate (processList):
                q.put(processParams)
                if i%2 ==0 and i>0:
                    for i in range(q.qsize()):
                        worker = Thread(target=self.execDong, args=(q,))
                        worker.setDaemon(True)
                        worker.start()
                    q.join()

            for i in range(q.qsize()):
                worker = Thread(target=self.execDong, args=(q,))
                worker.setDaemon(True)
                worker.start()
            q.join()
        else:
            p("THERE IS NO MODEL TO EXTRACT", "w")
        p('FINISHED TO EXTRACT AND LOAD >>>>>', "i")
        p('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', "ii")

    def execDong (self, q):
        while True:
            try:
                (jMap, procNum, procTotal) = q.get()
                dingObject = ddManager(node=jMap)
                if procTotal > 1:
                    p("DONG PROCESS NUMBER %s OUT OF %s" % (str(procNum), str(procTotal)))
                dingObject.dong()
            except Exception as e:
                err = traceback.extract_tb(e.__traceback__)
                p("MULTI THREADING ERROR:\n%s " % e, "e")
                for er in err:
                    p(er , "e")
            finally:
                q.task_done()

    def setLoggingLevel (self, val):
        CRITICAL = 50
        ERROR = 40
        WARNING = 30
        INFO = 20
        DEBUG = 10
        NOTSET = 0

        if val in (CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET):
            config.LOGS_DEBUG = val
            LOGGER_OBJECT.logLevel = val
            LOGGER_OBJECT.setLogLevel(logLevel=config.LOGS_DEBUG)
        else:
            err = "Logging is not valid, valid values: 0,10,20,30,40,50"
            raise ValueError(err)

    """ LOADING BUSINESS LOGIC EXECUTOERS """
    def execDbSql(self, queries, connName=None, connType=None, connUrl=None, connPropDic=None):
        connPropDic = connPropDic if connPropDic else {}
        if connName : connPropDic[eJson.jValues.NAME] = connName
        if connType : connPropDic[eJson.jValues.CONN] = connType
        if connUrl  : connPropDic[eJson.jValues.URL] = connUrl

        connObj = conn(connPropDic=connPropDic , connLoadProp=self.connDict)
        execQuery(sqlWithParamList=queries, connObj=connObj, msg=self.msg)

    def test (self):
        for connProp in self.connDict:
            c = conn(connPropDic=self.connDict[connProp], connLoadProp=None)
            c.test ()



    def execMicrosoftOLAP (self, serverName, dbName, cubes=[], dims=[], fullProcess=True):
        OLAP_Process(serverName=serverName, dbName=dbName, cubes=cubes, dims=dims, fullProcess=fullProcess)
        self.msg.addStateCnt()

    def __getNodes(self, destList=None, jsName=None, jsonNodes=None):
        allNodes = []
        if jsName and jsonNodes:
            allNodes = [(jsName, jsonNodes)]
        else:
            for jsonMapping in self.jsonParser.jsonMappings(destList=destList):
                for jsName in jsonMapping:
                    allNodes.append ( (jsName, jsonMapping[jsName]) )
        return allNodes