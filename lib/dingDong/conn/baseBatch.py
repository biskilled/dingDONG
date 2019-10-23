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
import copy


from dingDong.misc.logger   import p
from dingDong.misc.enumsJson import eConn, eJson, findProp
from dingDong.misc.misc     import replaceStr, uniocdeStr
from dingDong.config        import config


DEFAULTS    =   {
                    eJson.jValues.DEFAULT_TYPE:eConn.dataType.B_STR,
                    eJson.jValues.BATCH_SIZE:200000,
                    eJson.jValues.UPDATABLE:False
                }

# eConn.dataType.B_DEFAULT:'nvarchar(200)',
DATA_TYPES = {
    eConn.dataType.B_STR: {
                            eConn.dataType.DB_VARCHAR:None,
                            eConn.dataType.DB_NVARCHAR:None,
                            eConn.dataType.DB_CHAR:None,
                            eConn.dataType.DB_BLOB:None},
    eConn.dataType.B_INT: {eConn.dataType.DB_INT:None,
                           eConn.dataType.DB_BIGINT:None},
    eConn.dataType.B_FLOAT:{eConn.dataType.DB_FLOAT:None,
                            eConn.dataType.DB_DECIMAL:None},
    eConn.dataType.DB_DATE:{eConn.dataType.DB_DATE:None}
}

""" baseConn -- get connection propertirs : conn, connUrl,  connExtraUrl, connName,connDataTypes, connDefaultType, connPropDic"""
@six.add_metaclass(abc.ABCMeta)
class baseBatch ():
    def __init__ (self, conn=None, connName=None, connPropDict=None, defaults=None, dataType=None, update=None, versionManager=None):
        self.connPropDict   = connPropDict
        self.conn           = self.setProperties (propKey=eJson.jValues.CONN, propVal=conn)
        self.connName       = self.setProperties (propKey=eJson.jValues.NAME, propVal=connName)
        self.usingSchema    = True
        self.update         = self.setProperties (propKey=eJson.jValues.UPDATE, propVal=update, propDef=-1 )
        self.creeateFromObjName = self.setProperties (propKey=eJson.jValues.CREATE )
        self.versionManager = versionManager

        isObjectCanBeUpdate = self.connPropDict[eJson.jValues.UPDATABLE] if eJson.jValues.UPDATABLE in self.connPropDict else DEFAULTS[eJson.jValues.UPDATABLE]

        if not isObjectCanBeUpdate and self.update == eJson.jUpdate.UPDATE:
            config.DING_ADD_OBJECT_DATA = True
            self.update = eJson.jUpdate.DROP
        elif isObjectCanBeUpdate and self.update == eJson.jUpdate.UPDATE:
            config.DING_ADD_OBJECT_DATA = False

        if self.update == eJson.jUpdate.NO_UPDATE:
            config.DING_ADD_OBJECT_DATA = False

        if self.versionManager:
            p("VERSION MANAGER IS SET", "ii")


        if not self.conn:
            self.conn = self.connName

        self.baseDefaults   = DEFAULTS
        self.baseDataTypes  = copy.deepcopy(DATA_TYPES)
        self.baseDefDataType = self.baseDefaults[eJson.jValues.DEFAULT_TYPE]

        self.DEFAULTS   = self.setDefaults(defaultsDic=defaults)
        self.defDataType= self.DEFAULTS[eJson.jValues.DEFAULT_TYPE]

        self.DATA_TYPES = self.setDataTypes(connDataTypes=dataType)
        self.batchSize  = self.DEFAULTS[eJson.jValues.BATCH_SIZE]

        if not findProp (prop=self.conn, obj=eConn):
            err  = "Connection type is not valid: %s, use valid connection properties" %(str(self.conn))
            raise ValueError(err)

    def __tmpVer (self, s):
                p("!!! NOT UPDATE : %s" %str(s) )

    """ ABSTRACT METHOD THAT MUST BE IMPLEMETED IN ANY CHILD CLASSES """
    @abc.abstractmethod
    def connect(self):
        raise NotImplemented("baseConn-> Connect Method is not implemented !!!")

    @abc.abstractmethod
    def close(self):
        pass

    def test(self):
        if self.connect():
            p("TEST-> SUCCESS: %s, type: %s " % (self.connName, self.conn))
        else:
            p("TEST-> FAILED: %s, type: %s " % (self.connName, self.conn))

    @abc.abstractmethod
    def isExists(self, **args):
        pass

    @abc.abstractmethod
    def create(self, stt=None, objName=None, addIndex=None):
        pass

    @abc.abstractmethod
    def getStructure(self, **args):
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
    def execMethod(self, method=None):
        pass

    @abc.abstractmethod
    def merge(self, mergeTable, mergeKeys=None, sourceTable=None):
        pass

    @abc.abstractmethod
    def cntRows(self, objName=None):
        pass

    @abc.abstractmethod
    def createFrom (self, stt=None, objName=None, addIndex=None):
        pass

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
                            colVal = uniocdeStr(colVal,decode=True ) if colVal else ''
                            newVal = replaceStr(sString=str(newVal), findStr=match.group(1), repStr=colVal, ignoreCase=False, addQuotes=None)
                    row[ind] = newVal
                data[num] = row

        ## ceOBDC - convert data to None
        if self.conn == eConn.SQLSERVER:
            for num, row in enumerate(data):
                data[num] = [i if i!='' else None for i in row]
        return data

    """ GET DATA TYPE TREE - Return source data types"""
    def getDataTypeTree (self, dataType, dataTypeTree=None, ret=list([])):
        dataTypeTree = dataTypeTree if dataTypeTree else  copy.copy(self.DATA_TYPES)
        for k in dataTypeTree:
            k = str(k)
            if k.lower() == dataType.lower():
                ret.append(k)
                return ret
            if isinstance(dataTypeTree[k], dict):
                retDic = self.getDataTypeTree(dataType=dataType, dataTypeTree=dataTypeTree[k], ret=ret)
                if retDic:
                    ret.append(k)
                    return ret
            elif isinstance(dataTypeTree[k], list):
                lowerDataType = [x.lower() for x in dataTypeTree[k]]
                if dataType.lower() in lowerDataType:
                    ret.append(str(dataType))
                    ret.append(k)
                    return ret
            elif dataType == dataTypeTree[k] or str(dataType).lower() == str(dataTypeTree[k]).lower():
                ret.append(str(dataType))
                ret.append(k)
                return ret
            elif dataType == k or str(dataType).lower() == str(k).lower():
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
                    if allDataTypes[k] and eConn.dataType.B_DEFAULT in allDataTypes[k]:
                        ret.append(allDataTypes[k][eConn.dataType.B_DEFAULT])
                    else:
                        ret.append(None)

                    dataTypeTree.remove(k)
                    return self.setDataTypeTree(dataTypeTree=dataTypeTree, allDataTypes=allDataTypes[k], ret=ret)
        return ret

    """ return default data types dictionary   """
    def setDefaults(self, defaultsDic):
        defValues = {}
        if defaultsDic and eConn.NONO in defaultsDic and defaultsDic[eConn.NONO]:
            defValues = defaultsDic[eConn.NONO]

        if defaultsDic and self.conn in defaultsDic and defaultsDic[self.conn]:
            defValues.update(defaultsDic[self.conn])

        elif defaultsDic and len(defValues) == 0:
            defValues = defaultsDic

        for k in self.baseDefaults:
            if k not in defValues:
                defValues[k] = self.baseDefaults[k]
        return defValues

    """ Set DataTypes tree --> Adding new children """
    def setDataTypes (self, connDataTypes, existsDataTypes=None):
        existsDataTypes = self.baseDataTypes.copy() if not existsDataTypes else existsDataTypes
        connDataTypes = connDataTypes[self.conn] if self.conn in connDataTypes else connDataTypes
        if connDataTypes and len(connDataTypes)>0:
            for k in existsDataTypes:
                if k in connDataTypes:
                    existsDataTypes[k] = copy.copy(connDataTypes[k])
                elif existsDataTypes[k] and isinstance(existsDataTypes[k], dict):
                    self.setDataTypes (connDataTypes=connDataTypes, existsDataTypes=existsDataTypes[k])
        return existsDataTypes

    """ 
        propKey - property key to serch in connPropDict (default) or in propDict 
        if connProp is exists - return connProp
        if connPropDic is Dictionary and connKey in - return connPropDic value
        if no connProp and connKey not found in connPropDic return defValue
     """
    def setProperties (self, propKey, propVal=None, propDict=None, propDef=None):
        propDict = propDict if propDict else self.connPropDict

        if propVal and isinstance(propVal, dict) and propKey in propVal and propVal[propKey] is not None:
            return propVal[propKey]
        elif propVal is not None and not isinstance(propVal, dict):
            return propVal
        elif propDict and propKey in propDict and propDict[propKey] is not None:
            return propDict[propKey]
        elif isinstance(propDef, dict) and propKey in propDef:
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