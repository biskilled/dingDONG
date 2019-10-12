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

from collections import OrderedDict

from dingDong.misc.logger     import p
from dingDong.misc.enumsJson import eConn, eJson
from dingDong.config import config

from dingDong.conn.connGlobalDB import baseGlobalDb
from dingDong.conn.connAccess   import access
from dingDong.conn.connMongo    import mongo
from dingDong.conn.connFile     import connFile

CLASS_TO_LOAD = {eConn.SQLSERVER :baseGlobalDb,
                 eConn.ORACLE:baseGlobalDb,
                 eConn.VERTICA:baseGlobalDb,
                 eConn.ACCESS:access,
                 eConn.MYSQL:baseGlobalDb,
                 eConn.LITE:baseGlobalDb,
                 eConn.FILE:connFile,
                 eConn.MONGO:mongo,
                 eConn.POSTGESQL:baseGlobalDb}

def addPropToDict (existsDict, newProp):
    if newProp and isinstance(newProp, (dict, OrderedDict)):
        for k in newProp:
            if k in existsDict and isinstance(newProp[k], dict):
                existsDict = addPropToDict (existsDict, newProp=newProp[k])
            elif k not in existsDict:
                existsDict[k] = newProp[k]
            elif k in existsDict and existsDict[k] is None:
                existsDict[k] = newProp[k]
    elif isinstance(newProp,str):
        existsDict[eJson.jValues.URL] = newProp
    else:
        p("THERE IS AN ERROR ADDING %s INTO DICTIONARY " %(newProp), "e")

    return existsDict

def mngConnectors(connPropDic, connLoadProp=None):

    connLoadProp = connLoadProp if connLoadProp else config.CONN_URL

    ## Merge by CONNECTION
    if eJson.jValues.CONN in connPropDic and connPropDic[eJson.jValues.CONN] in connLoadProp:
        connPropDic = addPropToDict(existsDict=connPropDic, newProp=connLoadProp[connPropDic[eJson.jValues.CONN]])
        # update Connection type
        if eJson.jValues.CONN in connLoadProp[connPropDic[eJson.jValues.CONN]] and connLoadProp[connPropDic[eJson.jValues.CONN]][eJson.jValues.CONN] is not None:
            connPropDic[eJson.jValues.CONN] = connLoadProp[connPropDic[eJson.jValues.CONN]][eJson.jValues.CONN]

    if eJson.jValues.NAME in connPropDic and connPropDic[eJson.jValues.NAME] in connLoadProp :
        connPropDic = addPropToDict(existsDict=connPropDic, newProp=connLoadProp[connPropDic[eJson.jValues.NAME]])

    elif eJson.jValues.TYPE in connPropDic and connPropDic[eJson.jValues.TYPE] in connLoadProp :
        connPropDic = addPropToDict(existsDict=connPropDic, newProp=connLoadProp[connPropDic[eJson.jValues.TYPE]])

    if eJson.jValues.NAME not in connPropDic or connPropDic[eJson.jValues.NAME] is None:
        connPropDic[eJson.jValues.NAME] = connPropDic[eJson.jValues.CONN]

    # Tal: if ther is no URL, check if conn is Set, search if there is only one conn and update details
    if  eJson.jValues.URL not in connPropDic or (connPropDic[eJson.jValues.URL] is None and connPropDic[eJson.jValues.CONN] is not None):
        connToLook  = connPropDic[eJson.jValues.CONN]

        # will match / add missing values from config_url. based on connection type
        for c in connLoadProp:
            if c == connToLook or (eJson.jValues.CONN in connLoadProp[c] and connToLook == connLoadProp[c][eJson.jValues.CONN] ):
                connParams = connLoadProp[c]

                for prop in connParams:
                    if prop not in connPropDic:
                        connPropDic[prop] = connParams[prop]
                        p("CONN: %s, ADD PROPERTY %s, VALUE: %s" % (str(connToLook), str(prop), str(connParams[prop])),"ii")
                    elif connParams[prop] is None and connParams[prop] is not None:
                        connParams[prop] = connParams[prop]
                        p("CONN: %s, UPDATE PROPERTY %s, VALUE: %s" % (str(connToLook), str(prop), str(connParams[prop])),"ii")


    if connPropDic and isinstance(connPropDic, dict) and eJson.jValues.CONN in connPropDic:
        cType = connPropDic[eJson.jValues.CONN]
        if cType in CLASS_TO_LOAD:
            return CLASS_TO_LOAD[cType]( connPropDict=connPropDic )

        else:
            p("CONNECTION %s is NOT DEFINED. PROP: %s" % (str(cType), str(connPropDic)), "e")
    else:
        p ("connectorMng->mngConnectors: must have TYPE prop. prop: %s " %(str(connPropDic)), "e")

def __queryParsetIntoList(self, sqlScript, getPython=True, removeContent=True, dicProp=None, pythonWord="popEtl"):
        if isinstance(sqlScript, (tuple, list)):
            sqlScript = "".join(sqlScript)
        # return list of sql (splitted by list of params)
        allQueries = self.__getAllQuery(longStr=sqlScript, splitParam=['GO', u';'])

        if getPython:
            allQueries = self.__getPythonParam(allQueries, mWorld=pythonWord)

        if removeContent:
            allQueries = self.__removeComments(allQueries)

        if dicProp:
            allQueries = self._replaceProp(allQueries, dicProp)

        return allQueries