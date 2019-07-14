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

import re
import copy
from collections import OrderedDict

from dingDong.misc.logger       import p
from dingDong.bl.jsonParser     import jsonParser
from dingDong.misc.enumsJson    import eJson
from dingDong.conn.baseConnectorManager   import mngConnectors as conn

## Execters
from dingDong.executers.executeSql import execQuery
from dingDong.executers.executeAddMsg import executeAddMsg

class dingDong:
    def __init__ (self,  dicObj=None, filePath=None,
                dirData=None, includeFiles=None, notIncludeFiles=None,
                connDict=None):

        self.jsonParser = jsonParser(dicObj=dicObj, filePath=filePath,
                                     dirData=dirData, includeFiles=includeFiles, notIncludeFiles=notIncludeFiles,
                                     connDict=connDict)

        self.msg = executeAddMsg()

        ## Defualt properties
        self.connDict       = self.jsonParser.connDict
        self.stt            = None
        self.addSourceColumn= True
        self.src            = None
        self.tar            = None
        self.mrg            = None
        self.execProc       = None

        self.mergeSource    = None
        self.mergeTarget    = None
        self.mergeKeys      = None

    def ding (self, destList=None, jsName=None, jsonNodes=None):
        p('STARTING TO MODEL DATA STRUCURE >>>>>' , "i")
        if jsName and jsonNodes:
            allNodes = [(jsName, jsonNodes)]
        else:
            allNodes = [(jsName, jsonNodes) for jsName, jsonNodes in self.__getNodes(destList=destList)]

        for jsName, jsonNodes in allNodes:
            mergeSource = None
            for jMap in jsonNodes:
                self.__setSTT(node=jMap)
                sourceStt = None
                targetStt = None
                for node in jMap:
                    if isinstance(jMap[node], (list, tuple)):
                        self.ding(destList=None, jsName=jsName, jsonNodes=jMap[node])
                        continue

                    if not isinstance(jMap[node], (dict, OrderedDict)):
                        p("Not valid json values - must be list of dictionary.. continue. val: %s " % (str(jMap)),"e")
                        continue

                    self.__setMainProperty(key=node, valDict=jMap[node])

                    if self.src:
                        mergeSource = copy.copy(self.src)

                    if self.tar and self.src:
                        # convert source data type to target data types
                        targetStt = self.__updateTargetBySourceAndStt(src=self.src, tar=self.tar)

                        self.tar.create(stt=targetStt)
                        mergeSource = copy.copy(self.tar)
                        self.src.close()
                        self.tar.close()

                        sourceStt = None
                        self.src = None
                        self.tar = None

                    elif self.tar and self.stt:
                        self.tar.create(stt=self.stt)
                        mergeSource = copy.copy(self.tar)
                        self.tar.close()
                        self.tar = None

                    if self.mrg and mergeSource:
                        sttMerge = self.__updateTargetBySourceAndStt(src=mergeSource, tar=self.mrg)
                        self.mrg.create(stt=sttMerge)
                        self.mrg.close()
                        self.mrg = None

        p('FINSHED TO MODEL DATA STRUCURE >>>>>', "i")
        p('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', "ii")

    def dong (self, destList=None, jsName=None, jsonNodes=None):
        p('STARTING TO TRANSFER DATA STRUCURE >>>>>', "i")
        if jsName and jsonNodes:
            allNodes = [(jsName, jsonNodes)]
        else:
            allNodes = [(jsName, jsonNodes) for jsName, jsonNodes in self.__getNodes(destList=destList)]

        for jsName, jsonNodes in allNodes:
            self.src = None
            self.tar = None
            self.mrg = None
            self.execProc = None
            self.stt = None
            self.addSourceColumn = True

            for jMap in jsonNodes:
                self.__setSTT(node=jMap)

                for node in jMap:
                    if isinstance(jMap[node], (list, tuple)):
                        self.dong(destList=destList, jsName=jsName, jsonNodes=jMap[node])
                        continue

                    if not isinstance(jMap[node], (dict, OrderedDict)):
                        p("Not valid json values - must be list of dictionary.. continue. val: %s " % (str(jMap)),"e")
                        continue

                    self.__setMainProperty(key=node, valDict=jMap[node])

                    if self.execProc:
                        """ Execute internal connection procedure """
                        self.execProc = None

                    if self.src and self.tar:
                        """ TRANSFER DATA FROM SOURCE TO TARGET """
                        self.tar.preLoading()

                        tarToSrc = self.__mappingLoadingSourceToTarget(src=self.src, tar=self.tar)
                        self.src.extract(tar=self.tar, tarToSrc=tarToSrc, batchRows=10000, addAsTaret=True)
                        self.tar.close()
                        self.src.close()

                        self.src = None
                        self.tar = None

                    """ MERGE DATA BETWEEN SOURCE AND TARGET TABLES """
                    if self.mrg and self.mergeSource:
                        self.mrg.merge(mergeTable=self.mergeTarget, mergeKeys=self.mergeKeys, sourceTable=None)
                        self.mrg.close()

                        self.mergeSource = None
                        self.mergeTarget = None
                        self.mergeKeys = None

        p('FINISHED TO TRANSFER DATA STRUCURE >>>>>', "i")
        p('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', "ii")

    """ LOADING BUSINESS LOGIC EXECUTOERS """
    def execDbSql(self, queries, connName=None, connType=None, connUrl=None, connLoadProp=None):
        connPropDic = {eJson.jValues.NAME:connName, eJson.jValues.CONN:connType, eJson.jValues.URL:connUrl}
        connObj = conn(connPropDic=connPropDic, connLoadProp=connLoadProp)
        execQuery(sqlWithParamList=queries, connObj=connObj)


    """ Check Source values in STT - remove invalid values """
    def __updateSTTBySource (self, srcStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        srcStrucureL = {x.replace(pre,"").replace(pos,"").lower():x for x in srcStructure}

        removeColumns = []
        for col in self.stt:
            if eJson.jSttValues.SOURCE in self.stt[col] and self.stt[col][eJson.jSttValues.SOURCE].replace(pre,"").replace(pos,"").lower() not in srcStrucureL:
                removeColumns.append (col)

        for col in removeColumns:
            p("STT TAREGT %s HAVE INVALID SOURCE %s --> ignore COLUMN " % (col, self.stt[col][eJson.jSttValues.SOURCE]),"e")
            del self.stt[col]

    """ Check Taret values in STT - remove invalid values  """
    def __updateSTTByTarget (self, tarStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        tarStrucureL = {x.replace(pre,"").replace(pos,"").lower():x for x in tarStructure}
        for col in self.stt:
            if col.replace(pre,"").replace(pos,"").lower() not in tarStrucureL:
                p("STT COLUMN %s NOT EXISTS OBJECT --> ignore COLUMN " %(col) ,"e")
                #del self.stt[col]

    def __setSTT (self, node):
        if eJson.jKeys.STTONLY in node:
            self.stt            = node[eJson.jKeys.STTONLY]
            self.addSourceColumn= False

        elif eJson.jKeys.STT in node:
            self.stt = node[eJson.jKeys.STT]
        else:
            self.stt            = {}
            self.addSourceColumn= True

    def __setMainProperty (self,key, valDict):
        if eJson.jKeys.EXEC == key or eJson.jKeys.EXEC in valDict:
            self.execProc = valDict

        elif eJson.jKeys.SOURCE == key or eJson.jKeys.SOURCE in valDict:
            valDict[eJson.jValues.IS_SOURCE] = True
            self.src = conn(connPropDic=valDict)

        elif eJson.jKeys.QUERY == key or eJson.jKeys.QUERY in valDict:
            valDict[eJson.jValues.IS_SOURCE] = True
            self.src = conn(connPropDic=valDict)

        elif eJson.jKeys.TARGET == key or eJson.jKeys.TARGET in valDict:
            valDict[eJson.jValues.IS_TARGET] = True
            self.tar = conn(connPropDic=valDict)

        elif eJson.jKeys.MERGE == key or eJson.jKeys.MERGE in valDict:
            self.mergeSource= valDict[eJson.jMergeValues.SOURCE]
            self.mergeTarget= valDict[eJson.jMergeValues.TARGET]
            self.mergeKeys  = valDict[eJson.jMergeValues.MERGE]

            if self.tar and self.tar.connObj == self.mergeSource:
                self.mrg = self.tar
            elif self.src and self.src.connObj == self.mergeSource:
                self.mrg = self.src
            else:
                p("MERGE SOURCE %s IS NOT FOUND, IGNORE MERGE " %(self.mergeSource),"e")
        elif eJson.jKeys.STTONLY == key or eJson.jKeys.STT == key:
            pass
        else:
            p("IGNORE NODE: KEY:%s, VALUE:%s " % (key,str(valDict)), "e")

    """ LOADING MODULE: Return mapping between source and target """
    def __mappingLoadingSourceToTarget (self, src, tar):
        tarToSrc        = OrderedDict()
        srcStructure    = src.getStructure()
        tarStructure    = tar.getStructure()

        srcPre, srcPos,tarPre, tarPos = '','','',''
        if hasattr(src, 'columnFrame'):
            srcPre, srcPos = src.columnFrame[0], src.columnFrame[1]

        if hasattr(tar, 'columnFrame'):
            tarPre, tarPos = tar.columnFrame[0], tar.columnFrame[1]


        # remove from STT column that not exists in Target OBJECT OR Source OBJECT
        if tar.usingSchema:
            self.__updateSTTByTarget(tarStructure=tarStructure, pre=tarPre, pos=tarPos)
        self.__updateSTTBySource(srcStructure=srcStructure, pre=srcPre, pos=srcPos)

        srcColumns = {}
        tarColumns = {x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in tarStructure}
        sttColumns = {x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in self.stt}

        ## {srcName in Target: Source column }
        for col in srcStructure:
            colAlias= srcStructure[col][eJson.jSttValues.ALIACE] if eJson.jSttValues.ALIACE in srcStructure[col] else None
            colName = colAlias if colAlias else col
            srcColumns[colName.replace(srcPre, "").replace(srcPos, "").lower()] = col

        # There is no target schema --> using all source and STT
        if not tar.usingSchema and self.addSourceColumn:
            for col in srcColumns:
                tarToSrc[col] = {eJson.jSttValues.SOURCE: srcColumns[col]}

        # ADD ALL COLUMN FROM SOURCE
        elif self.addSourceColumn:
            for col in tarColumns:
                if col in srcColumns:
                    tarToSrc[tarColumns[col]] = {eJson.jSttValues.SOURCE: srcColumns[col]}

        tarToSrcColumns = {x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in tarToSrc}
        for col in sttColumns:
            if col in tarToSrcColumns:
                tarToSrc[tarToSrcColumns[col]].update(self.stt[sttColumns[col]])
            else:
                tarToSrc[sttColumns[col]] = self.stt[sttColumns[col]]

        if not tar.usingSchema:
            p("TAREGT %s DO NOT HAVE FIX SCHEMA " %(tar.conn), "ii")
            return tarToSrc

        #### Check Column in Source and not exists in mapping
        existsTarColumns = {}
        existsSrcColumns = {}

        for col in tarToSrc:
            existsTarColumns[col.replace(tarPre, "").replace(tarPos, "").lower()] = col
            if eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE]:
                existsSrcColumns[
                    tarToSrc[col][eJson.jSttValues.SOURCE].replace(srcPre, "").replace(srcPos, "").lower()] = \
                tarToSrc[col][eJson.jSttValues.SOURCE]

        columnNotMapped = list([])
        for col in tarColumns:
            if col not in existsTarColumns:
                columnNotMapped.append(tarColumns[col])
        if len(columnNotMapped) > 0:
            p("TARGET COLUMN NOT MAPPED: %s" % (str(columnNotMapped)), "ii")


        columnNotMapped = list([])
        for col in srcColumns:
            if srcColumns[col].replace(srcPre, "").replace(srcPos, "").lower() not in existsSrcColumns:
                columnNotMapped.append(srcColumns[col])
        if len(columnNotMapped) > 0:
            p("SOURCE COLUMN NOT MAPPED: %s" % (str(columnNotMapped)), "ii")

        return tarToSrc

    """ MAPPING MODULE: Convert Source Data Type into Target Data Type """
    def __updateTargetBySourceAndStt(self, src, tar):
        retStrucure = OrderedDict()
        sourceStt   = src.getStructure()

        sourceColL  = {x.lower():x for x in sourceStt}

        if src.conn == tar.conn:
            if self.addSourceColumn:
                for col in sourceStt:
                    if eJson.jSttValues.ALIACE in sourceStt[col] and sourceStt[col][eJson.jSttValues.ALIACE]:
                        retStrucure[ sourceStt[col][eJson.jSttValues.ALIACE] ] = {eJson.jSttValues.TYPE: sourceStt[col][eJson.jSttValues.TYPE]}
                    else:
                        retStrucure[col] = {eJson.jSttValues.TYPE: sourceStt[col][eJson.jSttValues.TYPE]}

        ### Source connection type is different than target connection type
        elif self.addSourceColumn:
            for col in sourceStt:
                targetColName = col
                if eJson.jSttValues.ALIACE in sourceStt[col] and sourceStt[col][eJson.jSttValues.ALIACE]:
                    targetColName = sourceStt[col][eJson.jSttValues.ALIACE]

                colType = sourceStt[col][eJson.jSttValues.TYPE] if eJson.jSttValues.TYPE in sourceStt[col] and sourceStt[col][eJson.jSttValues.TYPE] else tar.defDataType
                fmatch = re.search(r'(.*)(\(.+\))', colType, re.M | re.I)
                if fmatch:
                    replaceString   = fmatch.group(1)  # --> varchar, int , ...
                    postType        = fmatch.group(2)  # --> (X), (X,X) , ....
                else:
                    replaceString   = colType
                    postType        = ''

                ## Receive list of all dataType in DataTypes Tree
                newDataTypeTree     = src.getDataTypeTree (dataType=replaceString.lower(), ret=([]))
                if not newDataTypeTree:
                    tarType = tar.defDataType
                else:
                    targetList = tar.setDataTypeTree (dataTypeTree=newDataTypeTree, allDataTypes=tar.dataTypes, ret=[])
                    tarType = '%s%s' %(targetList[-1],postType) if targetList and len(targetList)>0 else tar.defDatatType
                retStrucure[targetColName] = {eJson.jSttValues.TYPE:tarType}

        for col in self.stt:
            if col.lower() in sourceColL and eJson.jSttValues.TYPE in self.stt[col] and self.stt[col][eJson.jSttValues.TYPE]:
                retStrucure[sourceColL[ col.lower() ]][eJson.jSttValues.TYPE] = self.stt[col][eJson.jSttValues.TYPE]

                if eJson.jSttValues.ALIACE in self.stt[col]:
                    retStrucure[sourceColL[col.lower()]][eJson.jSttValues.ALIACE] = self.stt[col][
                        eJson.jSttValues.ALIACE]

            elif col.lower() not in sourceColL:
                if eJson.jSttValues.TYPE in self.stt[col] and self.stt[col][eJson.jSttValues.TYPE]:
                    retStrucure[col] = {eJson.jSttValues.TYPE: self.stt[col][eJson.jSttValues.TYPE]}
                else:
                    retStrucure[col] = {eJson.jSttValues.TYPE: self.tar.defDataType}

                    if eJson.jSttValues.ALIACE in self.stt[col]:
                        retStrucure[col][eJson.jSttValues.ALIACE] = self.stt[col][eJson.jSttValues.ALIACE]

        return retStrucure

    def __getNodes(self, destList=None):
        for jsonMapping in self.jsonParser.jsonMappings(destList=destList):
            for jsName in jsonMapping:
                yield jsName, jsonMapping[jsName]
