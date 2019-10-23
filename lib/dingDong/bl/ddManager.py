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

from dingDong.misc.enumsJson    import eJson
from dingDong.misc.logger import p
from dingDong.conn.baseConnectorManager   import mngConnectors as conn
from dingDong.misc.misc         import uniocdeStr
from dingDong.config import config


class ddManager (object):
    def __init__(self, node, connDict=None, versionManager=None):
        self.stt            = None
        self.addSourceColumn= True
        self.addIndex       = None
        self.nodes          = None
        self.connDict       = connDict if connDict else config.CONN_URL
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
                if eJson.jKeys.STTONLY in node:
                    self.stt = node[eJson.jKeys.STTONLY]
                    self.addSourceColumn = False

                # ADD STT
                if eJson.jKeys.STT in node:
                    self.stt = node[eJson.jKeys.STT]
                    self.addSourceColumn = True

                    if eJson.jKeys.STTONLY in node:
                        self.addSourceColumn = False
                        for k in node[eJson.jKeys.STTONLY]:
                            if k not in self.stt:
                                self.stt[k] = node[eJson.jKeys.STTONLY][k]
                            else:
                                self.stt[k].update ( node[eJson.jKeys.STTONLY][k] )

                # ADD Index
                if eJson.jKeys.INDEX in node:
                    self.addIndex = node[eJson.jKeys.INDEX]

                for i,k in enumerate (node):
                    if eJson.jKeys.SOURCE == k or eJson.jKeys.SOURCE in node[k]:
                        node[k][eJson.jValues.IS_SOURCE] = True
                        modelDict[eJson.jKeys.SOURCE] = conn(connPropDic=node[k], connLoadProp=self.connDict, versionManager=self.versionManager)

                    elif eJson.jKeys.QUERY == k or eJson.jKeys.QUERY in node[k]:
                        if eJson.jKeys.SOURCE in modelDict:
                            p("THERE IS QUERY AND SOURCE, WILL USE QUERY AS SOURCE", "w")
                        node[k][eJson.jValues.IS_SOURCE] = True
                        modelDict[eJson.jKeys.SOURCE] = conn(connPropDic=node[k], connLoadProp=self.connDict, versionManager=self.versionManager)

                    elif eJson.jKeys.TARGET == k or eJson.jKeys.TARGET in node[k]:
                        node[k][eJson.jValues.IS_TARGET] = True
                        modelDict[eJson.jKeys.TARGET] = conn(connPropDic=node[k], connLoadProp=self.connDict, versionManager=self.versionManager)

                    elif eJson.jKeys.MERGE == k or eJson.jKeys.MERGE in node[k]:
                        modelDict[eJson.jKeys.MERGE] = node[k]

                    elif eJson.jKeys.STT == k or eJson.jKeys.STTONLY==k or eJson.jKeys.INDEX==k:
                        pass

                    elif eJson.jKeys.CREATE == k or eJson.jKeys.CREATE in node[k]:
                        modelDict[eJson.jKeys.CREATE] = conn(connPropDic=node[k], connLoadProp=self.connDict, versionManager=self.versionManager)

                    else:
                        modelDict[i] = conn(connPropDic=node[k], connLoadProp=self.connDict, versionManager=self.versionManager)
                orderedNodes.append(modelDict)

        return orderedNodes

    """ LOADING MODULE: Return mapping between source and target """
    def mappingLoadingSourceToTarget(self, src, tar):
        tarToSrc    = OrderedDict()
        srcStructure= src.getStructure()
        tarStructure= tar.getStructure()

        srcPre, srcPos, tarPre, tarPos = '', '', '', ''
        if hasattr(src, 'columnFrame'):
            srcPre, srcPos = src.columnFrame[0], src.columnFrame[1]

        if hasattr(tar, 'columnFrame'):
            tarPre, tarPos = tar.columnFrame[0], tar.columnFrame[1]

        # remove from STT column that not exists in Target OBJECT OR Source OBJECT

        self.__updateSTTBySourceOrTarget(srcStructure=srcStructure, pre=srcPre, pos=srcPos)
        srcColumns = OrderedDict()
        tarColumns = OrderedDict({x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in tarStructure})

        sttColumns = OrderedDict({x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in self.stt}) if self.stt else OrderedDict()

        ## {srcName in Target: Source column }
        for col in srcStructure:
            colAlias= col.replace(srcPre, "").replace(srcPos, "").lower()
            colName = srcStructure[col][eJson.jSttValues.SOURCE] if eJson.jSttValues.SOURCE in srcStructure[col] else col
            srcColumns[colAlias] = colName

        # There is no target schema --> using all source and STT
        if tar.usingSchema and self.addSourceColumn:
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
            colL = col.replace(tarPre, "").replace(tarPos, "").lower()
            existsTarColumns[colL] = col
            if eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE]:
                srcL = tarToSrc[col][eJson.jSttValues.SOURCE].replace(srcPre, "").replace(srcPos, "").lower()
                existsSrcColumns[srcL] = tarToSrc[col][eJson.jSttValues.SOURCE]

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
            srcPre, srcPos = src.columnFrame[0], src.columnFrame[1]
            for col in sourceStt:
                targetColName = col.replace(srcPre,"").replace(srcPos,"")
                if eJson.jSttValues.ALIACE in sourceStt[col] and sourceStt[col][eJson.jSttValues.ALIACE]:
                    targetColName = sourceStt[col][eJson.jSttValues.ALIACE].replace(srcPre,"").replace(srcPos,"")


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

                if newDataTypeTree is None:
                    p("SOURCE CONNECTION: %s, COLUMN: %s, DATA TYPE: %s ; IS NOT EXISTS, WILL USE DEFAULT VALUE" %(src.conn,col, replaceString),"w")
                    tarType = tar.defDataType
                else:
                    targetList = tar.setDataTypeTree (dataTypeTree=newDataTypeTree, allDataTypes=tar.DATA_TYPES, ret=[])
                    if len (targetList)>2:
                        targetList = [x for x in targetList if x]

                    tarType = '%s%s' %(targetList[-1],postType) if targetList and len(targetList)>0 and targetList[-1] is not None else tar.defDataType
                retStrucure[targetColName] = {eJson.jSttValues.TYPE:tarType}

        retStrucureL = {x.lower():x for x in retStrucure}
        retStrucureKeys = list (retStrucure.keys())

        if self.stt:
            for col in self.stt:
                if eJson.jSttValues.SOURCE in self.stt[col]:
                    sourceName = self.stt[col][eJson.jSttValues.SOURCE]
                    if sourceName in retStrucureKeys:
                        del retStrucure[sourceName]
                    elif sourceName.lower() in retStrucureL:
                        del retStrucure[retStrucureL[sourceName.lower()]]

                if col.lower() in sourceColL and eJson.jSttValues.TYPE in self.stt[col] and self.stt[col][eJson.jSttValues.TYPE]:
                    if sourceColL[ col.lower() ] not in retStrucure:
                        retStrucure[ sourceColL[ col.lower() ] ] = {}
                    retStrucure[sourceColL[ col.lower() ]] [eJson.jSttValues.TYPE] = self.stt[col][eJson.jSttValues.TYPE]

                    if eJson.jSttValues.ALIACE in self.stt[col]:
                        retStrucure[sourceColL[col.lower()]][eJson.jSttValues.ALIACE] = self.stt[col][
                            eJson.jSttValues.ALIACE]

                elif col.lower() not in sourceColL:
                    if eJson.jSttValues.TYPE in self.stt[col] and self.stt[col][eJson.jSttValues.TYPE]:
                        retStrucure[col] = {eJson.jSttValues.TYPE: self.stt[col][eJson.jSttValues.TYPE]}
                    else:
                        retStrucure[col] = {eJson.jSttValues.TYPE: tar.defDataType}

                        if eJson.jSttValues.ALIACE in self.stt[col]:
                            retStrucure[col][eJson.jSttValues.ALIACE] = self.stt[col][eJson.jSttValues.ALIACE]
        return retStrucure

    """ Check Source values in STT - remove invalid values """
    def __updateSTTBySourceOrTarget (self, srcStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        srcStrucureL = []
        srcColumns = {}

        for col in  srcStructure:
            srcStrucureL.append (col.replace(pre,"").replace(pos,"").lower())
            if eJson.jSttValues.SOURCE in srcStructure[col] and srcStructure[col][eJson.jSttValues.SOURCE]:
                srcStrucureL.append(uniocdeStr(srcStructure[col][eJson.jSttValues.SOURCE]))
                srcColumns[srcStructure[col][eJson.jSttValues.SOURCE]] = None

        removeColumnsSrc = []
        if self.stt:
            for col in self.stt:
                if eJson.jSttValues.SOURCE in self.stt[col] and self.stt[col][eJson.jSttValues.SOURCE] not in srcColumns:
                    if self.stt[col][eJson.jSttValues.SOURCE].replace(pre,"").replace(pos,"").lower() not in srcStrucureL:
                        removeColumnsSrc.append (col)

            for col in removeColumnsSrc:
                p("STT TAREGT %s HAVE INVALID SOURCE %s --> ignore COLUMN " % (col, self.stt[col][eJson.jSttValues.SOURCE]),"w")
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

                    if eJson.jKeys.SOURCE == k:
                        src = node[k]
                        mrgSource = src

                    elif eJson.jKeys.TARGET == k:
                        tar = node[k]
                        mrgSource = tar

                    elif eJson.jKeys.MERGE == k:
                        mrg = node[k]

                    if src and tar:
                        """ TRANSFER DATA FROM SOURCE TO TARGET """
                        tar.preLoading()
                        mrgSource = tar
                        tarToSrc = self.mappingLoadingSourceToTarget(src=src, tar=tar)
                        src.extract(tar=tar, tarToSrc=tarToSrc, addAsTaret=True)
                        tar.close()
                        src.close()
                        src = None

                    """ MERGE DATA BETWEEN SOURCE AND TARGET TABLES """
                    if mrg and mrgSource:
                        mrgSource.connect()
                        mergeTarget = mrg[eJson.jMergeValues.TARGET]
                        mergeKeys   = mrg[eJson.jMergeValues.MERGE]
                        mrgSource.merge(mergeTable=mergeTarget, mergeKeys=mergeKeys, sourceTable=None)
                        mrgSource.close()

    def ding( self ):
        if self.nodes and len(self.nodes) > 0:
            src         = None
            tar         = None
            mrgSource   = None
            for node in self.nodes:
                for k in node:
                    if eJson.jKeys.SOURCE == k:
                        src = node[k]
                        mrgSource = src

                    elif eJson.jKeys.TARGET == k:
                        tar = node[k]
                        mrgSource = tar

                        if  eJson.jKeys.SOURCE not in node:
                            tar.create(stt=self.stt, addIndex=self.addIndex)
                            tar.close()
                            tar = None

                    if tar and src:
                        # convert source data type to target data types
                        targetStt = self.updateTargetBySourceAndStt(src=src, tar=tar)


                        if targetStt and len(targetStt)>0:
                            tar.create(stt=targetStt, addIndex=self.addIndex)
                        else:
                            p("SOURCE: %s STRUCUTRE NOT DEFINED-> CANNOT CREATE TARGET" %(src.conn),"e")

                        mrgSource = tar
                        src.close()
                        tar.close()
                        src = None

                    if eJson.jKeys.MERGE == k and mrgSource:
                        mrgTarget = copy.copy(mrgSource)
                        mrgSource.connect()
                        mrgTarget.connect()
                        mrgTarget.connObj   = node[k][eJson.jMergeValues.TARGET]
                        mrgTarget.connIsTar = True
                        mrgTarget.connIsSrc = False
                        sttMerge = self.updateTargetBySourceAndStt(src=mrgSource, tar=mrgTarget)
                        mrgTarget.create(stt=sttMerge,addIndex=self.addIndex)
                        mrgTarget.close()
                        mrgSource.close()
                        mrgSource = None

                    if eJson.jKeys.CREATE == k:
                        node[k].createFrom(stt=self.stt, addIndex=self.addIndex)