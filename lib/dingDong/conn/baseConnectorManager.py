# (c) 2017-2019, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of popEye
#
# popEye is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# popEye is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with cadenceEtl.  If not, see <http://www.gnu.org/licenses/>.

from collections import OrderedDict

from dingDong.misc.logger     import p
from dingDong.misc.enumsJson import eConn, eJson
from dingDong.config import config

from dingDong.conn.connGlobalDB   import baseGlobalDb
from dingDong.conn.connAccess     import access
from dingDong.conn.connFile       import connFile
from dingDong.conn.dbSqlLite      import sqlLite

CLASS_TO_LOAD = {eConn.SQLSERVER :baseGlobalDb,
                 eConn.ORACLE:baseGlobalDb,
                 eConn.VERTICA:baseGlobalDb,
                 eConn.ACCESS:access,
                 eConn.MYSQL:baseGlobalDb,
                 eConn.LITE:sqlLite,
                 eConn.FILE:connFile}

def addPropToDict (existsDict, newProp):
    if newProp and isinstance(newProp, (dict, OrderedDict)):
        for k in newProp:
            if k in existsDict and isinstance(newProp[k], dict):
                existsDict = addPropToDict (existsDict, newProp=newProp[k])
            elif k not in existsDict:
                existsDict[k] = newProp[k]
            elif k in existsDict and not existsDict[k]:
                existsDict[k] = newProp[k]
            elif k in existsDict and existsDict[k] and existsDict[k]!=newProp[k]:
                p("connectorMng->addPropToDict: values change from %s to %s for prop: %s " %(str(existsDict[k]),str(newProp[k]), str(k)),"ii")
                existsDict[k] = newProp[k]
    elif isinstance(newProp,str):
        existsDict[eJson.jValues.URL] = newProp
    else:
        p("THERE IS AN ERROR ADDING %s INTO DICTIONARY " %(newProp), "e")

    return existsDict

def mngConnectors(connPropDic, connLoadProp=None):
    connLoadProp = connLoadProp if connLoadProp else config.CONN_URL

    if connLoadProp:
        if eJson.jValues.NAME in connPropDic and connPropDic[eJson.jValues.NAME] in connLoadProp :
            connPropDic = addPropToDict(existsDict=connPropDic, newProp=connLoadProp[connPropDic[eJson.jValues.NAME]])

        elif eJson.jValues.TYPE in connPropDic and connPropDic[eJson.jValues.TYPE] in connLoadProp :
            connPropDic = addPropToDict(existsDict=connPropDic, newProp=connLoadProp[connPropDic[eJson.jValues.TYPE]])

        elif eJson.jValues.CONN in connPropDic and connPropDic[eJson.jValues.CONN] in connLoadProp :
            connPropDic = addPropToDict(existsDict=connPropDic, newProp=connLoadProp[connPropDic[eJson.jValues.CONN]])

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