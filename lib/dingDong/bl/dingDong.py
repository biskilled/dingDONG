# (c) 2017-2019, Tal Shany <tal.shany@biSkilled.com>
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


class dingDong:
    def __init__ (self,  dicObj=None, filePath=None,
                dirData=None, includeFiles=None, notIncludeFiles=None,
                dirLogs=None,connDict=None, processes=None):

        self.jsonParser = jsonParser(dicObj=dicObj, filePath=filePath,
                                     dirData=dirData, includeFiles=includeFiles, notIncludeFiles=notIncludeFiles,
                                     connDict=connDict)

        self.msg = executeAddMsg()
        self.propcesses = processes if processes else config.NUM_OF_PROCESSES

        if dirLogs or config.LOGS_DIR:
            LOGGER_OBJECT.setLogsFiles (logDir=dirLogs)

        ## Defualt properties
        self.connDict       = self.jsonParser.connDict

    def ding (self, destList=None, jsName=None, jsonNodes=None):
        p('STARTING TO MODEL DATA STRUCURE >>>>>' , "i")
        allNodes = self.__getNodes(destList=destList, jsName=jsName, jsonNodes=jsonNodes)

        ## ALL Files
        for jsName, jsonNodes in allNodes:
            ## ALL On all nodes
            for jMap in jsonNodes:
                dingObject = ddManager(node=jMap)
                dingObject.ding()

        p('FINSHED TO MODEL DATA STRUCURE >>>>>', "i")
        p('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', "ii")

    ## There is parrallel processing option
    def dong (self, destList=None, jsName=None, jsonNodes=None):
        p('STARTING TO TRANSFER DATA STRUCURE >>>>>', "i")
        allNodes = self.__getNodes(destList=destList, jsName=jsName, jsonNodes=jsonNodes)
        processList = []

        for jsName, jsonNodes in allNodes:
            procTotal = len(jsonNodes)
            for procNum, jMap in  enumerate (jsonNodes):
                if self.propcesses<2:
                    dingObject = ddManager(node=jMap)
                    dingObject.dong()
                else:
                    processList.append ( (jMap, procNum, procTotal) )

        numOfProcesses = len(processList) if len(processList) < self.propcesses else self.propcesses

        if numOfProcesses > 1:
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
        p('FINISHED TO TRANSFER DATA STRUCURE >>>>>', "i")
        p('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', "ii")

    def execDong (self, q):
        while True:
            (jMap, procNum, procTotal) = q.get()
            dingObject = ddManager(node=jMap)
            if procTotal > 1:
                p("DONG PROCESS NUMBER %s OUT OF %s" % (str(procNum), str(procTotal)))
            dingObject.dong()
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
        execQuery(sqlWithParamList=queries, connObj=connObj)

    def execMicrosoftOLAP (self, serverName, dbName, cubes=[], dims=[], fullProcess=True):
        OLAP_Process(serverName=serverName, dbName=dbName, cubes=cubes, dims=dims, fullProcess=fullProcess)

    def __getNodes(self, destList=None, jsName=None, jsonNodes=None):
        allNodes = []
        if jsName and jsonNodes:
            allNodes = [(jsName, jsonNodes)]
        else:
            for jsonMapping in self.jsonParser.jsonMappings(destList=destList):
                for jsName in jsonMapping:
                    allNodes.append ( (jsName, jsonMapping[jsName]) )
        return allNodes