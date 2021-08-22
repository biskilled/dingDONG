# Copyright (c) 2017-2021, BPMK LTD (BiSkilled) Tal Shany <tal@biSkilled.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
#
# This file is part of dingDONG
#
# dingDONG is free software: you can redistribute it and/or modify
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

import re
import copy
from collections import OrderedDict

from dingDONG.misc.enums           import eJson, eConn
from dingDONG.misc.logger          import p
from dingDONG.conn.baseConnManager import mngConnectors as connManager
from dingDONG.misc.globalMethods import uniocdeStr
from dingDONG.config               import config


class nodeExec (object):
    def __init__(self, node, connDict=None, versionManager=None):
        self.stt            = None
        self.addSourceColumn= True
        self.addIndex       = None
        self.nodes          = None
        self.connDict       = connDict if connDict else config.CONNECTIONS
        self.versionManager = versionManager

        jsonNodes           = []

        if isinstance(node, (list,tuple)):
            jsonNodes  = node
        elif isinstance(node, (dict,OrderedDict)):
            jsonNodes = [node]
        else:
            p("NODE IS NOT LIST OR DICTIONARY, IGNORE NODE")

        ## INIT LIST NODES TO EXECUTE
        if len(jsonNodes)>0:
            self.nodes =  self.initNodes (jsonNodes)

    def initNodes (self, nodes):
        orderedNodes = []
        for node in nodes:
            modelDict = OrderedDict()
            if isinstance(node, (list, tuple)):
                orderedNodes.append (self.initNodes (nodes=node) )
            elif not isinstance(node, (dict, OrderedDict)):
                p("NODE IS NOT LIST OR DICT, IGNORE NODE\n%s" %str(node),"e")
            else:
                # ADD STT ONLY
                if eJson.STTONLY in node:
                    self.stt = node[eJson.STTONLY]
                    self.addSourceColumn = False

                # ADD STT
                if eJson.STT in node:
                    self.stt = node[eJson.STT]
                    self.addSourceColumn = True

                    if eJson.STTONLY in node:
                        self.addSourceColumn = False
                        for k in node[eJson.STTONLY]:
                            if k not in self.stt:
                                self.stt[k] = node[eJson.STTONLY][k]
                            else:
                                self.stt[k].update ( node[eJson.STTONLY][k] )

                # ADD Index
                if eJson.INDEX in node:
                    self.addIndex = node[eJson.INDEX]

                for i,k in enumerate (node):
                    if eJson.SOURCE == k or eJson.SOURCE in node[k]:
                        node[k][eConn.props.IS_SOURCE] = True
                        modelDict[eJson.SOURCE] = connManager(propertyDict=node[k], connLoadProp=self.connDict)

                    elif eJson.QUERY == k or eJson.QUERY in node[k]:
                        if eJson.SOURCE in modelDict:
                            p("THERE IS QUERY AND SOURCE, WILL USE QUERY AS SOURCE", "w")
                        node[k][eConn.props.IS_SOURCE] = True
                        modelDict[eJson.SOURCE] = connManager(propertyDict=node[k], connLoadProp=self.connDict)

                    elif eJson.TARGET == k or eJson.TARGET in node[k]:
                        node[k][eConn.props.IS_TARGET] = True
                        modelDict[eJson.TARGET] = connManager(propertyDict=node[k], connLoadProp=self.connDict)

                    elif eJson.MERGE == k or eJson.MERGE in node[k]:
                        modelDict[eJson.MERGE] = node[k]

                    elif eJson.STT == k or eJson.STTONLY==k or eJson.INDEX==k:
                        pass

                    elif eJson.CREATE == k or eJson.CREATE in node[k]:
                        modelDict[eJson.CREATE] = connManager(propertyDict=node[k], connLoadProp=self.connDict)
                    else:
                        modelDict[i] = connManager(propertyDict=node[k], connLoadProp=self.connDict)
                orderedNodes.append(modelDict)

        return orderedNodes

    """ LOADING MODULE: Return mapping between source and target """
    def mappingLoadingSourceToTarget(self, srcDictStructure,  src, tar):

        if not srcDictStructure:
            return None

        tarToSrc    = OrderedDict()

        srcPre, srcPos, tarPre, tarPos = '', '', '', ''
        if hasattr(src, 'columnFrame'):
            srcPre, srcPos = src.columnFrame[0], src.columnFrame[1]

        if hasattr(tar, 'columnFrame'):
            tarPre, tarPos = tar.columnFrame[0], tar.columnFrame[1]

        if  src.isSingleObject:
            srcDictStructure = {'':srcDictStructure}


        # remove from STT column that not exists in Target OBJECT OR Source OBJECT
        for src in srcDictStructure:
            srcStructure = srcDictStructure[src]

            if src and len(src)>0:
                tarStructure = tar.getStructure(objects=src)
            else:
                tarStructure = tar.getStructure()

            self.__updateSTTBySourceOrTarget(srcStructure=srcStructure, pre=srcPre, pos=srcPos)
            srcColumns = OrderedDict()
            if tarStructure and len(tarStructure)>0:
                tarColumns = OrderedDict({x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in tarStructure})
            else:
                tarColumns = []

            sttColumns = OrderedDict()
            if self.stt:
                for x in self.stt:
                    sttColumns[x.replace(tarPre, "").replace(tarPos, "").lower()] = x
            tarToSrc[src] = OrderedDict()

            ## {srcName in Target: Source column }
            for col in srcStructure:
                colAlias= col.replace(srcPre, "").replace(srcPos, "").lower()
                colName = srcStructure[col][eJson.stt.SOURCE] if eJson.stt.SOURCE in srcStructure[col] else col
                srcColumns[colAlias] = colName

            # There is no target schema --> using all source and STT
            if self.addSourceColumn:
                for col in srcColumns:
                    tarToSrc[src][col] = {eJson.stt.SOURCE: srcColumns[col]}

            else:
                for col in tarColumns:
                    if col in srcColumns:
                        tarToSrc[src][tarColumns[col]] = {eJson.stt.SOURCE: srcColumns[col]}

            tarToSrcColumns = {x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in tarToSrc[src]}

            for col in sttColumns:
                if col in tarToSrcColumns:
                    tarToSrc[src][tarToSrcColumns[col]].update(self.stt[sttColumns[col]])
                else:
                    tarToSrc[src][sttColumns[col]] = self.stt[sttColumns[col]]

            #### Check Column in Source and not exists in mapping
            existsTarColumns = {}
            existsSrcColumns = {}

            for col in tarToSrc[src]:
                colL = col.replace(tarPre, "").replace(tarPos, "").lower()
                existsTarColumns[colL] = col
                if eJson.stt.SOURCE in tarToSrc[src][col] and tarToSrc[src][col][eJson.stt.SOURCE]:
                    srcL = tarToSrc[src][col][eJson.stt.SOURCE].replace(srcPre, "").replace(srcPos, "").lower()
                    existsSrcColumns[srcL] = tarToSrc[src][col][eJson.stt.SOURCE]

            columnNotMapped = u""
            for col in tarColumns:
                if col not in existsTarColumns:
                    columnNotMapped+=uniocdeStr(tarColumns[col])+u" ; "

            if len(columnNotMapped) > 0:
                p(u"TARGET COLUMN NOT MAPPED: %s" % (columnNotMapped), "w")

            columnNotMapped = u""
            for col in srcColumns:
                if srcColumns[col].replace(srcPre, "").replace(srcPos, "").lower() not in existsSrcColumns:
                    columnNotMapped+=uniocdeStr(srcColumns[col])+u" ; "
            if len(columnNotMapped) > 0:
                p(u"SOURCE COLUMN NOT MAPPED: %s" % (columnNotMapped), "w")

        return tarToSrc

    def convertToTargetDataType (self, sttVal, src, tar ):
        srcPre, srcPos = src.columnFrame[0], src.columnFrame[1]
        newSttVal = OrderedDict()

        if src.connType == tar.connType:
            for col in sttVal:
                if eJson.stt.ALIACE in sttVal[col] and sttVal[col][eJson.stt.ALIACE]:
                    newSttVal[sttVal[col][eJson.stt.ALIACE]] = {eJson.stt.TYPE: sttVal[col][eJson.stt.TYPE]}
                else:
                    newSttVal[col] = {eJson.stt.TYPE: sttVal[col][eJson.stt.TYPE]}
        else:
            for col in sttVal:
                targetColName = col.replace(srcPre, "").replace(srcPos, "")
                if eJson.stt.ALIACE in sttVal[col] and sttVal[col][eJson.stt.ALIACE]:
                    targetColName = sttVal[col][eJson.stt.ALIACE].replace(srcPre, "").replace(srcPos, "")

                colType = sttVal[col][eJson.stt.TYPE] if eJson.stt.TYPE in sttVal[col] and sttVal[col][
                    eJson.stt.TYPE] else tar.defDataType
                fmatch = re.search(r'(.*)(\(.+\))', colType, re.M | re.I)
                if fmatch:
                    replaceString = fmatch.group(1)  # --> varchar, int , ...
                    postType = fmatch.group(2)  # --> (X), (X,X) , ....
                else:
                    replaceString = colType
                    postType = ''

                ## Receive list of all dataType in DataTypes Tree
                newDataTypeTree = src.getDataTypeTree(dataType=replaceString.lower(), ret=([]))

                if newDataTypeTree is None:
                    p("SOURCE CONNECTION: %s, COLUMN: %s, DATA TYPE: %s ; IS NOT EXISTS, WILL USE DEFAULT VALUE" % (
                    src.connType, col, replaceString), "w")
                    tarType = tar.defDataType
                else:
                    targetList = tar.setDataTypeTree(dataTypeTree=newDataTypeTree, allDataTypes=tar.dataTypes,
                                                     ret=[])
                    if len(targetList) > 2:
                        targetList = [x for x in targetList if x]

                    tarType = '%s%s' % (targetList[-1], postType) if targetList and len(targetList) > 0 and \
                                                                     targetList[-1] is not None else tar.defDataType
                newSttVal[targetColName] = {eJson.stt.TYPE: tarType}
        return newSttVal

    def addSTTProperties (self, srcDict, tar, onlyFromStt=False):
        srcKeys = {x.lower(): x for x in srcDict}

        ### UPDATE srcDict with STT values
        if self.stt:
            columnToRemoveFromSrc = []
            for colName in self.stt:
                ## if source exists in stt, remove original column and add new naming column
                if self.stt[colName] and eJson.stt.SOURCE in self.stt[colName]:
                    srcColumnNameInSTT = self.stt[colName][eJson.stt.SOURCE]
                    ## source exists
                    if srcColumnNameInSTT.lower() in srcKeys:
                        srcColumnName = srcKeys[srcColumnNameInSTT.lower()]
                        if colName.lower() not in srcKeys:
                            srcDict[colName] = self.stt[colName]
                            # Theere is new name to source column, will remove original name
                            for prop in srcDict [ srcColumnName ]:
                                if prop not in srcDict[colName]:
                                    srcDict[colName][prop] = srcDict[colName][prop]
                            columnToRemoveFromSrc.append ( srcColumnName )
                        else:
                            for prop in self.stt[colName]:
                                srcDict[ srcColumnName ][prop] = self.stt[colName][prop]
                    else:
                        if colName.lower() in srcKeys:
                            for prop in self.stt[colName]:
                                srcDict[ srcKeys[colName.lower()] ][prop] = self.stt[colName][prop]

                        else:
                            srcDict[colName] = self.stt[colName]

                elif colName.lower() not in srcKeys:
                    srcDict[colName] = self.stt[colName]

                else:
                    srcColumnName = srcKeys[colName.lower()]
                    for prop in self.stt[colName]:
                        srcDict[srcColumnName][prop] = self.stt[colName][prop]

                if eJson.stt.TYPE not in srcDict[colName] or not srcDict[colName][eJson.stt.TYPE]:
                    srcDict[colName][eJson.stt.TYPE] = tar.defDataType

            for column in columnToRemoveFromSrc:
                del srcDict[column]

        if onlyFromStt:
            removeColumn = filter(lambda x: (x not in self.stt), srcDict )
            for column in removeColumn:
                del srcDict[column]
        return srcDict

    """ MAPPING MODULE: Convert Source Data Type into Target Data Type """
    def updateTargetBySourceAndStt(self, src, tar):
        retListStrucure = OrderedDict()
        sourceStt       = src.getStructure()

        if src.isSingleObject:
            sourceStt= {'':sourceStt}

        for stt in sourceStt:
            srcDict = self.convertToTargetDataType(sttVal=sourceStt[stt], src=src, tar=tar)
            if self.addSourceColumn:
                srcDict = self.addSTTProperties(srcDict=srcDict, tar=tar, onlyFromStt=False)
            else:
                srcDict = self.addSTTProperties(srcDict=srcDict, tar=tar, onlyFromStt=True)
            retListStrucure[stt] = srcDict.copy()

        if len(retListStrucure)==1:
            k, v = retListStrucure.popitem()
            return v
        return retListStrucure

    """ Check Source values in STT - remove invalid values """
    def __updateSTTBySourceOrTarget (self, srcStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        srcStrucureL = []
        srcColumns = {}

        for col in srcStructure:
            srcStrucureL.append (col.replace(pre,"").replace(pos,"").lower())
            if eJson.stt.SOURCE in srcStructure[col] and srcStructure[col][eJson.stt.SOURCE]:
                srcName = srcStructure[col][eJson.stt.SOURCE].replace(pre,"").replace(pos,"")
                srcStrucureL.append(uniocdeStr(srcName))
                srcColumns[srcName] = None

        removeColumnsSrc = []
        if self.stt:
            for col in self.stt:
                if eJson.stt.SOURCE in self.stt[col] and self.stt[col][eJson.stt.SOURCE] not in srcColumns:
                    if self.stt[col][eJson.stt.SOURCE].replace(pre,"").replace(pos,"").lower() not in srcStrucureL:
                        removeColumnsSrc.append (col)

            for col in removeColumnsSrc:
                p("STT TAREGT %s HAVE INVALID SOURCE %s --> ignore COLUMN " % (col, self.stt[col][eJson.stt.SOURCE]),"w")
                del self.stt[col]

    def dong( self ):
        if self.nodes and len(self.nodes)>0:
            src         = None
            tar         = None
            mrg         = None
            mrgSource   = None
            for node in self.nodes:
                for k in node:
                    ## Exec METHOD
                    if str(k).isdigit():
                        node[k].execMethod()

                    if eJson.SOURCE == k:
                        if node[k] and node[k].test():
                            src = node[k]
                            mrgSource = src

                    elif eJson.TARGET == k:
                        if node[k] and node[k].test():
                            tar = node[k]
                            mrgSource = tar

                    elif eJson.MERGE == k:
                            mrg = node[k]

                    if src and tar:
                        """ TRANSFER DATA FROM SOURCE TO TARGET """
                        srcDictStructure = src.getStructure()
                        tar.preLoading(dictObj=srcDictStructure)

                        mrgSource = tar
                        tarToSrcDict = self.mappingLoadingSourceToTarget(srcDictStructure=srcDictStructure, src=src, tar=tar)

                        src.extract(tar=tar, tarToSrcDict=tarToSrcDict)
                        tar.close()
                        src.close()
                        src = None

                    """ MERGE DATA BETWEEN SOURCE AND TARGET TABLES """
                    if mrg and mrgSource:
                        mrgSource.connect()
                        if mrgSource.test():
                            mergeTarget = mrg[eJson.merge.TARGET]
                            mergeKeys   = mrg[eJson.merge.MERGE]
                            ignoreColumns=mrg[eJson.merge.IGNORE]
                            mrgSource.merge(mergeTable=mergeTarget, mergeKeys=mergeKeys, sourceTable=None, ignoreUpdateColumn=ignoreColumns)
                            mrgSource.close()

    def ding( self ):
        if self.nodes and len(self.nodes) > 0:
            src         = None
            tar         = None
            mrgSource   = None
            for node in self.nodes:
                for k in node:
                    if eJson.SOURCE == k:
                        if node[k] and node[k].test():
                            src = node[k]
                            mrgSource = src

                    elif eJson.TARGET == k:
                        if node[k] and node[k].test():
                            tar = node[k]
                            mrgSource = tar

                            if  eJson.SOURCE not in node:
                                tar.create(stt=self.stt, addIndex=self.addIndex)
                                tar.close()
                                tar = None

                    if tar and src:
                        # convert source data type to target data types
                        targetStt = self.updateTargetBySourceAndStt(src=src, tar=tar)

                        if targetStt and len(targetStt)>0:
                            tar.create(sttDict=targetStt, addIndex=self.addIndex)
                        else:
                            p("SOURCE: %s STRUCUTRE NOT DEFINED-> CANNOT CREATE TARGET" %(src.connType),"e")

                        mrgSource = tar
                        src.close()
                        tar.close()
                        src = None

                    if eJson.MERGE == k and mrgSource:
                        mrgTarget = copy.copy(mrgSource)
                        mrgSource.connect()
                        mrgTarget.connect()

                        if mrgSource.test() and mrgTarget.test():
                            mrgTarget.connTbl   = node[k][eJson.merge.TARGET]
                            mrgTarget.connIsTar = True
                            mrgTarget.connIsSrc = False
                            if eConn.updateMethod.UPDATE  in node[k]:
                                mrgTarget.update = node[k][eConn.updateMethod.UPDATE]
                            sttMerge = self.updateTargetBySourceAndStt(src=mrgSource, tar=mrgTarget)
                            mrgTarget.create(sttDict=sttMerge,addIndex=self.addIndex)
                            mrgTarget.close()
                            mrgSource.close()
                        mrgSource = None

                    if eJson.CREATE == k:
                        if node[k].test():
                            node[k].create(sttDict=self.stt, addIndex=self.addIndex)