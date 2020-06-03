# Copyright (c) 2017-2020, BPMK LTD (BiSkilled) Tal Shany <tal@biSkilled.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
#
# This file is part of dingDONG
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# dingDONG is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDONG.  If not, see <http://www.gnu.org/licenses/>.

import os
import io
import json

from collections import OrderedDict

from dingDONG.misc.logger import p
from dingDONG.misc.enums import eJson, eConn
from dingDONG.misc.globalMethods import findEnum, getAllProp
from dingDONG.config      import config

#xx = [[{'s':}],[{'prop':{'pp':45}},{}],{'sql1':'', 's':xxx,'t':'dsdsdsd'},{''},{}]
# s : [obj] [conn,  obj],[conn. obj, filter]  {"conn":conn, "obj":obj, filter:"dddd"} // connection:"s":[]

class jsonParser (object):

    """ Create list of node to load and init all connection dictionary """
    def __init__ (self, dicObj=None, filePath=None,
                  dirData=None, includeFiles=None, notIncludeFiles=None, connDict=None, sqlFolder=None):

        if connDict and isinstance(connDict ,(dict, OrderedDict)):
            config.CONNECTIONS.update(connDict)

        self.connDict = config.CONNECTIONS
        self.sqlFolder= sqlFolder
        self.__initConnDict ()
        self.listObj    = []
        self.listFiles  = []
        self.jsonMapp   = []
        self.jsonName   = ''
        self.dirData    = None
        msg     = ""

        if dicObj:
            self.listObj.append (dicObj)
            msg+="loading data from dictioanry; "

        if filePath:
            if os.path.isfile (filePath):
                with io.open(filePath, encoding="utf-8") as jsonFile:  #
                    dicObj = json.load(jsonFile, object_pairs_hook=OrderedDict)
                    self.listObj.append (dicObj)
                    msg+="Loading data from file"
            else:
                p("file %s is not exists " %(filePath),"e")

        if dirData:
            self.dirData = dirData
            if not os.path.isdir(dirData):
                p ("Folder not exists : %s" % dirData, "e")
                return
            else:
                msg += "loading data from folder: %s" %dirData

                jsonFiles = [pos_json for pos_json in os.listdir(dirData) if pos_json.endswith('.json')]
                jsonFilesDic = {x.lower().replace(".json", ""): x for x in jsonFiles}

                if notIncludeFiles:
                    notIncludeDict = {x.lower().replace(".json", ""): x for x in notIncludeFiles}
                    for f in jsonFilesDic:
                        if f in notIncludeDict:
                            p('NOT INCLUDE: File:%s, file: NOT IN USED, REMOVED >>>>' % (str( os.path.join(dirData,f) ) ), "ii")
                            jsonFiles.remove(jsonFilesDic[f])
                    for f in notIncludeDict:
                        if f not in jsonFilesDic:
                            p('NOT INCLUDE: File: %s, NOT EXISTS.Ignoring>>>>>' % ( str(os.path.join(dirData, f))), "ii")

                if includeFiles:
                    includeDict = {x.lower().replace(".json", ""): x for x in includeFiles}
                    for f in jsonFilesDic:
                        if f not in includeDict and f in jsonFilesDic and jsonFilesDic[f] in jsonFiles:
                            p('INCLUDE: Folder:%s, file: %s NOT IN USED, REMOVED >>>>' % (str(dirData), f), "ii")
                            jsonFiles.remove(jsonFilesDic[f])
                    for f in includeDict:
                        if f not in jsonFilesDic:
                            p('INCLUDE: Folder: %s, file: %s not exists.. Ignoring >>>>' % (str(dirData), f), "ii")
                self.listFiles = jsonFiles

    def jsonMappings (self, destList = None):
        if self.listObj and len (self.listObj)>0:
            self.jsonMapp = list([])
            self.jsonName = ''
            self.__initMetaDict(listObj=self.listObj, destList=destList)

            yield {self.jsonName:self.jsonMapp}
        elif self.listFiles and len (self.listFiles)>0:
            for index, js in enumerate(self.listFiles):
                self.jsonMapp = list([])
                self.jsonName = js
                with io.open(os.path.join(self.dirData, js), encoding="utf-8") as jsonFile:  #
                    jText = json.load(jsonFile, object_pairs_hook=OrderedDict)
                    self.__initMetaDict(listObj=jText, destList=destList)
                    yield {self.jsonName:self.jsonMapp}

    def getAllConnection (self, pr=True):
        if pr:
            for x in self.connDict:
                p('TYPE: %s\t\t\tPROP: %s' %(str(x), str(self.connDict[x])), "ii")
            p ("====================================================" , "ii")
        return self.connDict

    """ MAIN KEYS : NAME (n) AND CONNENCTION TYPE (conn) """
    def __initConnDict (self):
        newConnDict =  {}
        for conn in self.connDict:
            dictProp = {}
            dictProp[eConn.props.NAME] = conn
            dictProp[eConn.props.TYPE] = conn

            if isinstance( self.connDict[conn], dict ):
                for k in self.connDict[conn]:
                    origK = findEnum(prop=k, obj=eConn.props )
                    if origK:
                        dictProp[origK] = self.connDict[conn][k]
                    elif k.lower() in newConnDict:
                        dictProp[k.lower()] = self.connDict[conn][k]
                    else:
                        dictProp[k] = self.connDict[conn][k]

            elif isinstance( self.connDict[conn], str ):
                dictProp[eConn.props.URL] = self.connDict[conn]
            newConnDict[conn] = dictProp

            ### VALID CONNECTION TYPES
            errConn = []
            for conn in newConnDict:
                if not findEnum(prop=newConnDict[conn][eConn.props.TYPE], obj=eConn.types):
                    p("Remove Connection %s becouse prop: %s  is NOT VALID !!" %(str(conn), str(newConnDict[conn][eConn.props.TYPE])), "e")
                    errConn.append (conn)
            for err in errConn:
                del newConnDict[err]
        self.connDict = newConnDict

    # parse Json file into internal Json format
    # Key can be name or internal dictionary
    def __initMetaDict (self, listObj,destList=None):
        for node in listObj:
            if isinstance(node ,list):
                self.__initMetaDict(listObj=node, destList=destList)
            elif isinstance(node, dict):
                stt     = OrderedDict()
                newDict = OrderedDict()

                for prop in node:
                    k =  findEnum (prop=prop, obj=eJson )
                    if k:
                        if eJson.SOURCE in newDict and k == eJson.QUERY:
                            p("Source and Query exists - Will use Query", "ii")
                            del newDict[eJson.SOURCE]
                        elif eJson.QUERY in newDict and k == eJson.SOURCE:
                            p("Query and Source exists - Will use Source", "ii")
                            del newDict[eJson.QUERY]

                        if k == eJson.STT or k == eJson.STTONLY:
                            newSttL = {x.lower(): x for x in node[prop]}
                            sttL = {x.lower(): x for x in stt}
                            for s in stt:
                                if s.lower() in newSttL:
                                    stt[s].update(node[prop][newSttL[s.lower()]])

                            for s in newSttL:
                                if s not in sttL:
                                    stt[newSttL[s]] = node[prop][newSttL[s]]
                            newDict[k] =stt
                        # parse source / target / query
                        elif k == eJson.SOURCE or k == eJson.TARGET or k == eJson.QUERY:
                            newDict[k] = self.__sourceOrTargetOrQueryConn(propFullName=prop, propVal=node[prop])
                        # parse merge
                        elif k == eJson.MERGE:
                            newDict[k] = self.__mergeConn(existsNodes=newDict, propFullName=prop, propVal=node[prop])
                        # parse column data types
                        elif k == eJson.COLUMNS:
                            stt = self.__sttAddColumns (stt=stt, propVal=node[prop])
                            newDict[eJson.STTONLY] = stt

                            if eJson.STT in newDict:
                                del newDict[eJson.STT]

                        # parse column mapping
                        elif k == eJson.MAP:
                            stt = self.__sttAddMappings(stt=stt, propVal=node[prop])
                            if eJson.STT in newDict:
                                newDict[eJson.STT] = stt
                            else:
                                newDict[eJson.STTONLY] = stt
                        elif k == eJson.INDEX:
                            index = self.__index(propVal=node[prop])
                            if index:
                                newDict[eJson.INDEX] = index
                        elif k == eJson.CREATE:
                            newDict[k] = self.__createFrom(propVal=node[prop])
                        else:
                            p ("%s not implemented !" %(k), "e")
                    else:
                        newDict[prop.lower()] = self.__uniqueProc(propVal=node[prop])

                self.jsonMapp.append ( newDict )
            else:
                p("jsonParser->__initMetaDict: Unknown node prop values: %s " %node)

    ### List option : [obj] [conn,  obj],[conn. obj, filter]  // connection Name defoult
    ### FINAL : {eJK.CONN:None, eJK.OBJ:None, eJK.FILTER:None, eJK.URL:None, eJK.URLPARAM:None}
    def __sourceOrTargetOrQueryConn(self, propFullName, propVal):
        ret = {}
        if isinstance(propVal, str):
            ret[eConn.props.NAME] = propFullName
            ret[eConn.props.TYPE] = propFullName
            ret[eConn.props.TBL]  = propVal
        elif isinstance(propVal, list):
            ret[eConn.props.NAME] = propFullName
            ret[eConn.props.TYPE] = propFullName
            if len(propVal) == 1:
                ret[eConn.props.TYPE] = propFullName
                ret[eConn.props.TBL] = propVal[0]
            elif len(propVal) == 2:
                ret[eConn.props.TYPE] = propVal[0]
                ret[eConn.props.TBL]  = propVal[1]
            elif len(propVal) == 3:
                ret[eConn.props.TYPE]  = propVal[0]
                ret[eConn.props.TBL]   = propVal[1]

                if self.__isDigitStr(propVal[2]):
                    ret[eConn.props.UPDATE] =  self.__setUpdate (propVal[2])
                else:
                    ret[eConn.props.FILTER]= propVal[2]
            elif len(propVal) == 4:
                ret[eConn.props.TYPE]     = propVal[0]
                ret[eConn.props.TBL]      = propVal[1]
                ret[eConn.props.FILTER]   = propVal[2]
                ret[eConn.props.UPDATE]   = self.__setUpdate (propVal[3])

            else:
                p("%s: Not valid list valuues, must 1,2 or 3 VALUE IS: \n %s" % (str(propFullName),str(propVal)), "e")
        elif isinstance(propVal, dict):
            ret = self.__notVaildProp( currentPropDic=propVal, enumPropertyClass=eConn.props)

            if eConn.props.NAME not in ret and eConn.props.TYPE in ret:
                ret[ eConn.props.NAME ] = ret[ eConn.props.TYPE ]

            if eConn.props.TYPE not in ret and eConn.props.NAME in ret:
                ret[ eConn.props.TYPE ] = ret[ eConn.props.NAME ]

        else:
            p("Not valid values: %s " %(propVal),"e")
            return {}

        if findEnum (prop=ret[eConn.props.NAME], obj=eJson)  == eJson.QUERY:
            ret[eConn.props.IS_SQL] = True
        return ret

    # Must have source / target / query in existing nodes
    # [obj], [obj, keys]
    def __mergeConn (self, existsNodes, propFullName, propVal):
        ret     = eJson.merge.DIC.copy()
        srcConn = None

        if eJson.TARGET in existsNodes:
            srcConn = existsNodes[eJson.TARGET]
        elif eJson.SOURCE in existsNodes:
            srcConn = existsNodes[eJson.SOURCE]
        elif eJson.QUERY in existsNodes:
            srcConn = existsNodes[eJson.QUERY]

        if not srcConn:
            p("There is no query/ source or target to merge with. quiting. val: %s " %(str(propVal)) ,"e")
            return {}

        ### Update values from Source connection
        ret[eJson.merge.SOURCE]  = srcConn[eConn.props.TBL]
        ret[eConn.props.TYPE]    = srcConn[eConn.props.TYPE]

        if isinstance(propVal, str):
            ret[eConn.props.NAME]         = propFullName
            ret[eJson.merge.TARGET]  = propVal

        elif isinstance(propVal, list):
            ret[eConn.props.NAME] = propFullName
            if len(propVal) == 1:
                ret[eJson.merge.TARGET]  = propVal[0]
            elif len(propVal) == 2:
                ret[eJson.merge.TARGET] = propVal[0]
                if str(propVal[1]).isdigit():
                    ret[ eConn.props.UPDATE] = self.__setUpdate(propVal[1])
                else:
                    ret[eJson.merge.MERGE]  = propVal[1]
            elif len(propVal) == 3:
                ret[eJson.merge.TARGET]  = propVal[0]
                ret[eJson.merge.MERGE]   = propVal[1]
                ret[eConn.props.UPDATE]       = self.__setUpdate (propVal[2])
            else:
                p("%s: Not valid merge valuues, must have obj and merge key..." % (str(propVal)), "e")
        elif isinstance(propVal, dict):
            self.__notVaildProp( currentPropDic=propVal, enumPropertyClass=eJson.merge )
        else:
            p("Not valid values")

        ret[ eConn.props.TBL ] = ret[eJson.merge.TARGET]

        return ret

    def __setUpdate (self, val):
        if str(val).isdigit():
            if findEnum(prop=val, obj=eConn.updateMethod):
                return val
            else:
                p("THERE IS %s WHICH IS MAPPED TO UPDATE PROPERTY, MUST HAVE -1(drop), 1(UPDATE), 2(NO_UPDATE), USING -1 DROP--> CREATE METHOD ")
        return -1

    # Insert into STT Column mapping
    def __sttAddColumns(self, stt, propVal):
        if not isinstance(propVal, dict):
            p ("jsonParser->__sttAddColumns: Not valid prop %s, must be dictionary type" %(propVal),"e")
            return stt

        existsColumnsDict   = {x.lower():x for x in stt.keys()}


        for tar in propVal:
            if tar.lower() in existsColumnsDict:
                stt[ existsColumnsDict[tar.lower()] ][eJson.stt.TYPE] = propVal[tar]
            else:
                stt[tar] = {eJson.stt.TYPE:propVal[tar]}
        return stt

    # Insert / Add new column types
    def __sttAddMappings(selfself, stt, propVal):
        if not isinstance(propVal, dict):
            p ("jsonParser->__sttAddMappings: Not valid prop %s, must be dictionary type" %(propVal),"e")
            return stt
        existsColumnsDict = {x.lower(): x for x in stt.keys()}
        for tar in propVal:
            if tar.lower() in existsColumnsDict:
                stt[ existsColumnsDict[tar.lower()] ][eJson.stt.SOURCE] = propVal[tar]
            else:
                stt[tar][eJson.stt.SOURCE] = propVal[tar]
        return stt

    # Special connection execution
    # list - ]
    def __uniqueProc(self, propVal):
        ret= {}
        if isinstance(propVal ,list) and len(propVal)==2:
            ret[eConn.props.TYPE]     = propVal[0]
            ret[eConn.props.TBL]      = propVal[1]
            ret[eConn.props.FOLDER]   = self.sqlFolder

        elif isinstance(propVal ,dict) and eConn.props.TYPE in propVal and eConn.props.TBL in propVal:
            ret = propVal

        if eConn.props.TBL in ret and ret[eConn.props.TBL] is not None and '.sql' in ret[eConn.props.TBL]:
            fileName = ret[eConn.props.TBL]
            if os.path.isfile(fileName):
                ret[eConn.props.FILE] = fileName
            if eConn.props.FOLDER in ret and ret[eConn.props.FOLDER] is not None:
                folderPath = ret[eConn.props.FOLDER]
                if os.path.isfile( os.path.join(folderPath, fileName)):
                    ret[eConn.props.FILE] = os.path.join(folderPath, fileName)

        if eConn.props.TBL in ret and isinstance(ret[eConn.props.TBL], (list,tuple)):
            ret[eConn.props.TBL] = "".join(ret[eConn.props.TBL])

        return ret

    def __index (self, propVal):
        ## propVal = [{col:[],'iu':True, 'ic':True},{}]
        if isinstance(propVal, (dict,OrderedDict)):
            propVal = [propVal]

        if not isinstance(propVal, list):
            p("INDEX VALUES MUST BE A DICTIONARY OR LIST OF DICTIOANRY, FORMAT {'C'':list_column_index, 'ic':is cluster (True/False), 'iu': is unique (True/False)}")
            return
        ret = []
        for indexDict in propVal:

            if not isinstance(indexDict, (dict, OrderedDict)):
                p("INDEX MUST BE DICTIOANY, FORMAT {'C'':list_column_index, 'ic':is cluster (True/False), 'iu': is unique (True/False)}")
                continue

            returnDict = {eConn.props.DB_INDEX_COLUMS:[],eConn.props.DB_INDEX_CLUSTER:True,eConn.props.DB_INDEX_UNIQUE:False}
            for node in indexDict:
                k =  findEnum (prop=node.lower(), obj=eJson.index)

                if not k:
                    p("INDEX VALUES IS NOT VALID: %s, IGNORE INDEX. VALID FORMAT: FORMAT {'C'':list_column_index, 'ic':is cluster (True/False), 'iu': is unique (True/False)}")
                    break

                if k == eConn.props.DB_INDEX_COLUMS:
                    if isinstance(indexDict[node], list ):
                        returnDict[eConn.props.DB_INDEX_COLUMS].extend(indexDict[node])
                    else:
                        returnDict[eConn.props.DB_INDEX_COLUMS].append(indexDict[node])
                elif k == eConn.props.DB_INDEX_CLUSTER:
                    if not indexDict[node]: returnDict[eConn.props.DB_INDEX_CLUSTER] = False
                elif k == eConn.props.DB_INDEX_UNIQUE:
                    if indexDict[node]: returnDict[eConn.props.DB_INDEX_UNIQUE] = True
                else:
                    p("INDEX - UNRECOGNIZED KEY %s IN DICT:%s IGNORE. VALID FORMAT: FORMAT {'C'':list_column_index, 'ic':is cluster (True/False), 'iu': is unique (True/False)}" %(str(node),str(indexDict)), "e")
            ret.append(indexDict)
        return ret

    def __createFrom(self, propVal):
        ret = OrderedDict()
        if isinstance(propVal, str):
            ret[eConn.props.TYPE] = propVal
        elif isinstance(propVal, (tuple, list)):
            ret[eConn.props.TYPE] = propVal[0]
            ret[eConn.props.TBL]  = propVal[1]
        else:
            p("CREATE VALUES MUST BE STRING (connection name) OR LIST [connection name, object name], NOT VALID VALUES:%s" %str(propVal),"e" )
        return ret

    def __notVaildProp(self, currentPropDic, enumPropertyClass):
        ret = {}
        for k in currentPropDic:
            prop = findEnum(prop=k, obj=enumPropertyClass)
            if not prop:
                p("%s: NOT VALID. LEGAL VALUES: %s -> ignore" % (k, str(getAllProp(enumPropertyClass))), "e")

            ret[prop] = currentPropDic[k]
        return ret

    def __isDigitStr(self, x):
        try:
            int(str(x))
            return True
        except ValueError:
            return False