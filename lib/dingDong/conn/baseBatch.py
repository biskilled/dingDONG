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

import abc
import six
import sys
import re

from dingDong.misc.logger import p
from dingDong.misc.enumsJson import eConn, eJson, findProp

DEFAULTS    =   {
                    eJson.jValues.DEFAULT_TYPE:eConn.dataType.B_STR
                }

DATA_TYPES = {
    eConn.dataType.B_STR: {eConn.dataType.DB_VARCHAR:None,
                           eConn.dataType.DB_NVARCHAR:None,
                           eConn.dataType.DB_CHAR:None,
                           eConn.dataType.DB_BLOB:None},
    eConn.dataType.B_INT: {eConn.dataType.DB_INT:None,
                           eConn.dataType.DB_BIGINT:None},
    eConn.dataType.B_FLOAT:{eConn.dataType.DB_FLOAT:None,
                            eConn.dataType.DB_NUMERIC:None,
                            eConn.dataType.DB_DECIMAL:None},
    eConn.dataType.DB_DATE:{eConn.dataType.DB_DATE:None}
}

""" baseConn -- get connection propertirs : conn, connUrl,  connExtraUrl, connName,connDataTypes, connDefaultType, connPropDic"""
@six.add_metaclass(abc.ABCMeta)
class baseBatch ():
    def __init__ (self, conn=None, connName=None, connPropDict=None):
        self.connPropDict   = connPropDict
        self.conn           = self.setProperties (propKey=eJson.jValues.CONN, propVal=conn)
        self.connName       = self.setProperties (propKey=eJson.jValues.NAME, propVal=connName)
        self.usingSchema    = True

        if not self.conn:
            self.conn = self.connName

        self.defaults   = DEFAULTS
        self.dataTypes  = DATA_TYPES
        self.defDataType = self.defaults[eJson.jValues.DEFAULT_TYPE]

        if not findProp (prop=self.conn, obj=eConn):
            err  = "Connection type is not valid: %s, use valid connection properties" %(str(self.conn))
            raise ValueError(err)


    """ ABSTRACT METHOD THAT MUST BE IMPLEMETED IN ANY CHILD CLASSES """
    @abc.abstractmethod
    def connect(self):
        raise NotImplemented("baseConn-> Connect Method is not implemented !!!")

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def create(self, stt=None, objName=None):
        pass

    @abc.abstractmethod
    def getStructure(self, **args):
        pass

    @abc.abstractmethod
    def isExists(self, **args):
        pass

    @abc.abstractmethod
    def preLoading (self):
        pass

    @abc.abstractmethod
    def extract(self, **args):
        pass

    @abc.abstractmethod
    def load(self, **args):
        pass

    @abc.abstractmethod
    def execMethod(self):
        pass

    @abc.abstractmethod
    def merge(self, mergeTable, mergeKeys=None, sourceTable=None):
        pass

    """ PROPERTIES TO USE """
    @property
    def DEFAULTS (self):
        return self.defaults

    @DEFAULTS.setter
    def DEFAULTS(self, val):
        defValues = {}
        if eConn.NONO in val and val[eConn.NONO]:
            defValues = val[eConn.NONO]

        if self.conn in val and val[self.conn]:
            defValues.update(val[self.conn])
        elif len(defValues)==0:
            defValues = val

        if defValues and isinstance(defValues, dict):
            for k in defValues:
                self.defaults[k] = defValues[k]
        else:
            p("DEFAULTS VALUES ARE NOT VALID, MUST USE DICT, VALUES: %s " %str(val))

        self.defDataType = self.defaults[eJson.jValues.DEFAULT_TYPE]

    @property
    def DATA_TYPES(self):
        return self.dataTypes

    @DATA_TYPES.setter
    def DATA_TYPES(self, val):
        self.dataTypes = self.setDataTypes(connDataTypes=val)

    """ -----------------   GLOBAL METHODS -------------------------------------"""
    """ General method - implemented localy """
    def dataTransform(self, data, functionDict=None, execDict=None):
        regex = r"(\{.*?\})"
        if (functionDict and len(functionDict) > 0) or (execDict and len(execDict) > 0):
            for num, dataRow in enumerate(data):
                row = list(dataRow)
                for ind in functionDict:
                    newVal = row[ind]
                    for fn in functionDict[ind]:
                        newVal = fn.handler(newVal, ind)
                    row[ind] = newVal

                for ind in execDict:
                    newVal = execDict[ind]
                    matches = re.finditer(regex, execDict[ind], re.MULTILINE | re.DOTALL)
                    for matchNum, match in enumerate(matches):
                        for groupNum in range(0, len(match.groups())):
                            colNum = match.group(1).replace('{', '').replace('}', '')
                            colVal = row[int(colNum)]
                            if isinstance(colVal, str):
                                colVal = "'%s'" % (colVal) if "'" not in colVal else '"%s"' % (colVal)
                                newVal.replace(match.group(1), colVal)
                    row[ind] = newVal
                data[num] = row
        return data

    def test(self):
        if self.connect():
            p("TEST-> SUCCESS: %s, type: %s " % (self.connName, self.conn))
        else:
            p("TEST-> FAILED: %s, type: %s " % (self.connName, self.conn))

    """ GET DATA TYPE TREE - Return source data types"""
    def getDataTypeTree (self, dataType,dataTypeTree=None, ret=list([])):
        dataTypeTree = dataTypeTree if dataTypeTree else self.dataTypes
        for k in dataTypeTree:
            if isinstance(dataTypeTree[k], dict):
                retDic = self.getDataTypeTree(dataType=dataType, dataTypeTree=dataTypeTree[k], ret=ret)
                if retDic:
                    ret.append(k)
                    return ret
            elif isinstance(dataTypeTree[k], list):
                if dataType in dataTypeTree[k]:
                    ret.append(dataType)
                    ret.append(k)
                    return ret
            elif dataType == dataTypeTree[k]:
                ret.append(dataType)
                ret.append(k)
                return ret
            elif dataType == k:
                ret.append(k)
                return ret
        return None

    """ SET DARA TYPE - Return last known data type in target """
    def setDataTypeTree(self, dataTypeTree, allDataTypes, ret=[]):
        if not dataTypeTree or len(dataTypeTree)==0:
            return ret

        if isinstance(allDataTypes, list):
            for k in allDataTypes:
                if k in dataTypeTree:
                    ret.append(k)
                    dataTypeTree.remove(k)
                    return ret
            return ret
        elif allDataTypes and allDataTypes in dataTypeTree:
            return ret.append(allDataTypes)
        elif isinstance(allDataTypes, dict):
            for k in allDataTypes:
                if k in dataTypeTree:
                    ret.append(k)
                    dataTypeTree.remove(k)
                    return self.setDataTypeTree(dataTypeTree=dataTypeTree, allDataTypes=allDataTypes[k], ret=ret)
        return ret

    """ Set DataTypes tree --> Adding new children """
    def setDataTypes (self, connDataTypes, existsDataTypes=None):
        existsDataTypes = self.dataTypes if not existsDataTypes else existsDataTypes
        connDataTypes = connDataTypes[self.conn] if self.conn in connDataTypes else connDataTypes
        if connDataTypes and len(connDataTypes)>0:
            for k in existsDataTypes:
                if k in connDataTypes:
                    existsDataTypes[k] = connDataTypes[k]
                elif existsDataTypes[k] and isinstance(existsDataTypes[k], dict):
                    self.setDataTypes (connDataTypes=connDataTypes, existsDataTypes=existsDataTypes[k])
        return existsDataTypes

    """ 
        propKey - property key to serch in connPropDict (default) or in propDict 
        if connProp is exists - return connProp
        if connPropDic is Dictionary and connKey in - return connPropDic value
        if no connProp and connKey not found in connPropDic return defValue
     """
    def setProperties (self, propKey, propVal, propDict=None, propDef=None):
        propDict = propDict if propDict else self.connPropDict

        if propVal and isinstance(propVal, dict) and propKey in propVal and propVal[propKey] is not None:
            return propVal[propKey]
        elif propVal is not None and not isinstance(propVal, dict):
            return propVal
        elif propDict and propKey in propDict and propDict[propKey] is not None:
            return propDict[propKey]
        elif isinstance(propDef, dict) and propKey in propDef and propDef[propKey] is not None:
            return propDef[propKey]
        return propDef

    def decodeStrPython2Or3(self, sObj, un=True, decode="windows-1255"):
        pVersion = sys.version_info[0]

        if 3 == pVersion:
            return sObj
        else:
            if un:
                return unicode(sObj)
            elif decode:
                return str(sObj).decode(decode)

