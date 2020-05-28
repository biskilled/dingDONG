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

import re
import copy
from collections import OrderedDict

from dingDONG.misc.enums           import eJson, eConn
from dingDONG.misc.logger          import p
from dingDONG.conn.baseConnManager import mngConnectors as conn
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
                        modelDict[eJson.SOURCE] = conn(propertyDict=node[k], connLoadProp=self.connDict)

                    elif eJson.QUERY == k or eJson.QUERY in node[k]:
                        if eJson.SOURCE in modelDict:
                            p("THERE IS QUERY AND SOURCE, WILL USE QUERY AS SOURCE", "w")
                        node[k][eConn.props.IS_SOURCE] = True
                        modelDict[eJson.SOURCE] = conn(propertyDict=node[k], connLoadProp=self.connDict)

                    elif eJson.TARGET == k or eJson.TARGET in node[k]:
                        node[k][eConn.props.IS_TARGET] = True
                        modelDict[eJson.TARGET] = conn(propertyDict=node[k], connLoadProp=self.connDict)

                    elif eJson.MERGE == k or eJson.MERGE in node[k]:
                        modelDict[eJson.MERGE] = node[k]

                    elif eJson.STT == k or eJson.STTONLY==k or eJson.INDEX==k:
                        pass

                    elif eJson.CREATE == k or eJson.CREATE in node[k]:
                        modelDict[eJson.CREATE] = conn(propertyDict=node[k], connLoadProp=self.connDict)

                    else:
                        modelDict[i] = conn(propertyDict=node[k], connLoadProp=self.connDict)
                orderedNodes.append(modelDict)

        return orderedNodes

    """ LOADING MODULE: Return mapping between source and target """
    def mappingLoadingSourceToTarget(self, srcDictStructure,  src, tar):
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
            tarColumns = OrderedDict({x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in tarStructure})

            sttColumns = OrderedDict({x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in self.stt}) if self.stt else OrderedDict()
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

    """ MAPPING MODULE: Convert Source Data Type into Target Data Type """
    def updateTargetBySourceAndStt(self, src, tar):
        retListStrucure = OrderedDict()
        sourceStt       = src.getStructure()

        if src.isSingleObject:
            sourceStt= {'':sourceStt}

        if self.addSourceColumn:
            if src.connType == tar.connType:
                for stt in sourceStt:
                    sttVal      = sourceStt[stt]
                    newSttVal   = OrderedDict()

                    for col in sttVal:
                        if eJson.stt.ALIACE in sttVal[col] and sttVal[col][eJson.stt.ALIACE]:
                            newSttVal [ sttVal[col][eJson.stt.ALIACE] ] = {eJson.stt.TYPE: sttVal[col][eJson.stt.TYPE]}
                        else:
                            newSttVal[col] = {eJson.stt.TYPE: sttVal[col][eJson.stt.TYPE]}
                    retListStrucure[stt] = newSttVal.copy()
            else:
                srcPre, srcPos = src.columnFrame[0], src.columnFrame[1]
                for stt in sourceStt:
                    sttVal = sourceStt[stt]
                    newSttVal = OrderedDict()

                    for col in sttVal:
                        targetColName = col.replace(srcPre, "").replace(srcPos, "")
                        if eJson.stt.ALIACE in sttVal[col] and sttVal[col][eJson.stt.ALIACE]:
                            targetColName = sttVal[col][eJson.stt.ALIACE].replace(srcPre, "").replace(srcPos, "")

                        colType = sttVal[col][eJson.stt.TYPE] if eJson.stt.TYPE in sttVal[col] and sttVal[col][eJson.stt.TYPE] else tar.defDataType
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
                            p("SOURCE CONNECTION: %s, COLUMN: %s, DATA TYPE: %s ; IS NOT EXISTS, WILL USE DEFAULT VALUE" % (src.connType, col, replaceString), "w")
                            tarType = tar.defDataType
                        else:
                            targetList = tar.setDataTypeTree(dataTypeTree=newDataTypeTree, allDataTypes=tar.dataTypes,
                                                             ret=[])
                            if len(targetList) > 2:
                                targetList = [x for x in targetList if x]

                            tarType = '%s%s' % (targetList[-1], postType) if targetList and len(targetList) > 0 and \
                                                                             targetList[-1] is not None else tar.defDataType
                        newSttVal[targetColName] = {eJson.stt.TYPE: tarType}
                    retListStrucure[stt] = newSttVal.copy()

        if self.stt and len (self.stt)>0:
            for sstt in retListStrucure:
                newSttVal = retListStrucure[sstt]
                retStrucureL = {x.lower(): x for x in newSttVal}

                for col in self.stt:
                    colName = col
                    ## if source exists in stt, remove original column and add new naming column
                    if eJson.stt.SOURCE in self.stt[col]:
                        sourceName = self.stt[col][eJson.stt.SOURCE]

                        if sourceName.lower() in retStrucureL:
                            if col.lower() not in retStrucureL:
                                newSttVal[col] = self.stt[col]
                                del newSttVal[ retStrucureL[ sourceName.lower()] ]
                            else:
                               colName = retStrucureL[col.lower()]
                               for prop in self.stt[col]:
                                   newSttVal[ colName ][prop] = self.stt[col][prop]
                        else:
                            if col.lower() in retStrucureL:
                                for prop in self.stt[col]:
                                    newSttVal[ retStrucureL[col.lower()] ][prop] = self.stt[col][prop]

                            else:
                                newSttVal[col] =  self.stt[col]

                    elif col.lower() not in retStrucureL:
                        newSttVal[col] = self.stt[col]

                    else:
                        for prop in self.stt[col]:
                            colName = retStrucureL[col.lower()]
                            newSttVal[ colName ][prop] = self.stt[col][prop]


                    if eJson.stt.TYPE not in newSttVal[ colName ] or not newSttVal[ colName ][eJson.stt.TYPE]:
                        newSttVal[colName][eJson.stt.TYPE] = tar.defDataType

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
                srcStrucureL.append(uniocdeStr(srcStructure[col][eJson.stt.SOURCE]))
                srcColumns[srcStructure[col][eJson.stt.SOURCE]] = None

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
                        src = node[k]
                        mrgSource = src

                    elif eJson.TARGET == k:
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
                        mergeTarget = mrg[eJson.merge.TARGET]
                        mergeKeys   = mrg[eJson.merge.MERGE]
                        mrgSource.merge(mergeTable=mergeTarget, mergeKeys=mergeKeys, sourceTable=None)
                        mrgSource.close()

    def ding( self ):
        if self.nodes and len(self.nodes) > 0:
            src         = None
            tar         = None
            mrgSource   = None
            for node in self.nodes:
                for k in node:
                    if eJson.SOURCE == k:
                        src = node[k]
                        mrgSource = src

                    elif eJson.TARGET == k:
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
                        mrgTarget.connObj   = node[k][eJson.merge.TARGET]
                        mrgTarget.connIsTar = True
                        mrgTarget.connIsSrc = False
                        if eConn.updateMethod.UPDATE  in node[k]:
                            mrgTarget.update = node[k][eConn.updateMethod.UPDATE]
                        sttMerge = self.updateTargetBySourceAndStt(src=mrgSource, tar=mrgTarget)
                        mrgTarget.create(stt=sttMerge,addIndex=self.addIndex)
                        mrgTarget.close()
                        mrgSource.close()
                        mrgSource = None

                    if eJson.CREATE == k:
                        node[k].createFrom(stt=self.stt, addIndex=self.addIndex)