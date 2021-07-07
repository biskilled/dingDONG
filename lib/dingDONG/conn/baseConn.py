# (c) 2017-2020, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of dingDONG
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any late r version.
#
# dingDONG is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDONG.  If not, see <http://www.gnu.org/licenses/>.


import abc
import six
import copy
import re
from collections import OrderedDict

from dingDONG.misc.enums import eConn
from dingDONG.misc.globalMethods import setProperty, replaceStr, uniocdeStr

DEFAULTS = {
    eConn.defaults.DEFAULT_TYPE:eConn.dataTypes.B_STR,
    eConn.defaults.BATCH_SIZE:200000,
    eConn.defaults.UPDATABLE:False
    }

DATA_TYPES = {
    eConn.dataTypes.B_STR: None,
    eConn.dataTypes.B_INT: None,
    eConn.dataTypes.B_FLOAT:None,
    eConn.dataTypes.DB_DATE:None
    }

@six.add_metaclass(abc.ABCMeta)
class baseConn ():
    def __init__ (self, propertyDict=None, **args):

        if propertyDict:    args.update (propertyDict)
        self.propertyDict = args

        self.versionManager = setProperty(k=eConn.props.VERSION, o=self.propertyDict)

        self.defaults = DEFAULTS.copy()
        self.defaults.update (setProperty(k=eConn.props.DEFAULTS, o=self.propertyDict, defVal={}))

        self.dataTypes = self.setDataTypes (connDataTypes=setProperty(k=eConn.props.DATA_TYPES,o=self.propertyDict, defVal={})).copy()
        self.batchSize = self.defaults[eConn.defaults.BATCH_SIZE]

        self.objNames = OrderedDict()

    @abc.abstractmethod
    def connect(self):
        raise NotImplemented("{}-> Connect Method is not implemented !!!".format(self.__class__.__name__))

    def setDataTypes (self, connDataTypes, existsDataTypes=DATA_TYPES.copy()):
        if connDataTypes and len(connDataTypes)>0:
            for k in existsDataTypes:
                if k in connDataTypes:
                    existsDataTypes[k] = copy.copy(connDataTypes[k])
                elif existsDataTypes[k] and isinstance(existsDataTypes[k], dict):
                    self.setDataTypes (connDataTypes=connDataTypes, existsDataTypes=existsDataTypes[k])
        return existsDataTypes

    """ GET DATA TYPE TREE - Return source data types"""
    def getDataTypeTree(self, dataType, dataTypeTree=None, ret=list([])):
        dataTypeTree = dataTypeTree if dataTypeTree else copy.copy(self.dataTypes)
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
        if not dataTypeTree or len(dataTypeTree) == 0:
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
                    if allDataTypes[k] and eConn.dataTypes.B_DEFAULT in allDataTypes[k]:
                        ret.append(allDataTypes[k][eConn.dataTypes.B_DEFAULT])
                    else:
                        ret.append(None)

                    dataTypeTree.remove(k)
                    return self.setDataTypeTree(dataTypeTree=dataTypeTree, allDataTypes=allDataTypes[k], ret=ret)
        return ret

    """ -----------------   GLOBAL METHODS -------------------------------------"""
    """ General method - implemented localy """
    def dataTransform(self, data, functionDict=None, execDict=None):
        if isinstance(data, tuple):
            data = list(data)

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
        if self.connType == eConn.types.SQLSERVER:
            for num, row in enumerate(data):
                data[num] = [i if i!='' else None for i in row]
        return data

    def getStt (self,sttDict, k=None):
        if k and sttDict and k in sttDict:
            return sttDict[k]
        elif not k:
            return sttDict if sttDict else OrderedDict()
        return OrderedDict()