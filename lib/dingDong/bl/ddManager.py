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

from dingDong.misc.enumsJson    import eJson
from dingDong.misc.logger import p
from dingDong.conn.baseConnectorManager   import mngConnectors as conn
from dingDong.misc.misc         import uniocdeStr


class ddManager (object):
    def __init__(self, node):
        self.stt            = None
        self.addSourceColumn= True
        self.nodes          = None

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
                        p("CANNOT HAVE STT AND STT ONLY - WILL USE STT")

                        for k in node[eJson.jKeys.STTONLY]:
                            if k not in modelDict[eJson.jKeys.STT]:
                                self.stt[k] = node[eJson.jKeys.STTONLY][k]
                            else:
                                self.stt[k].update ( node[eJson.jKeys.STTONLY][k] )

                        modelDict[eJson.jKeys.STT].update (node[eJson.jKeys.STTONLY])

                for i,k in enumerate (node):
                    if eJson.jKeys.EXEC == k or eJson.jKeys.EXEC in node[k]:
                        modelDict[i] = conn(connPropDic=node[k])

                    elif eJson.jKeys.SOURCE == k or eJson.jKeys.SOURCE in node[k]:
                        node[k][eJson.jValues.IS_SOURCE] = True
                        modelDict[eJson.jKeys.SOURCE] = conn(connPropDic=node[k])

                    elif eJson.jKeys.QUERY == k or eJson.jKeys.QUERY in node[k]:
                        if eJson.jKeys.SOURCE in modelDict:
                            p("THERE IS QUERY AND SOURCE, WILL USE QUERY AS SOURCE", "w")
                        node[k][eJson.jValues.IS_SOURCE] = True
                        modelDict[eJson.jKeys.SOURCE] = conn(connPropDic=node[k])

                    elif eJson.jKeys.TARGET == k or eJson.jKeys.TARGET in node[k]:
                        node[k][eJson.jValues.IS_TARGET] = True
                        modelDict[eJson.jKeys.TARGET] = conn(connPropDic=node[k])

                    elif eJson.jKeys.MERGE == k or eJson.jKeys.MERGE in node[k]:
                        modelDict[eJson.jKeys.MERGE] = node[k]

                    elif eJson.jKeys.STT == k or eJson.jKeys.STTONLY==k:
                        pass
                    else:
                        p("IGNORE NODE: KEY:%s, VALUE:%s " % (k, str(node[k])), "e")
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
        if tar.usingSchema:
            self.__updateSTTByTarget(tarStructure=tarStructure, pre=tarPre, pos=tarPos)
        self.__updateSTTBySource(srcStructure=srcStructure, pre=srcPre, pos=srcPos)

        srcColumns = OrderedDict()
        tarColumns = OrderedDict({x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in tarStructure})

        sttColumns = OrderedDict({x.replace(tarPre, "").replace(tarPos, "").lower(): x for x in self.stt}) if self.stt else OrderedDict()

        ## {srcName in Target: Source column }
        for col in srcStructure:
            colAlias= srcStructure[col][eJson.jSttValues.ALIACE] if eJson.jSttValues.ALIACE in srcStructure[col] else None
            colName = colAlias if colAlias else col
            srcColumns[colName.replace(srcPre, "").replace(srcPos, "").lower()] = col

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
            existsTarColumns[col.replace(tarPre, "").replace(tarPos, "").lower()] = col
            if eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE]:
                existsSrcColumns[
                    tarToSrc[col][eJson.jSttValues.SOURCE].replace(srcPre, "").replace(srcPos, "").lower()] = \
                tarToSrc[col][eJson.jSttValues.SOURCE]

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
                if newDataTypeTree is None:
                    p("SOURCE CONNECTION: %s, COLUMN: %s, DATA TYPE: %s ; IS NOT EXISTS, WILL USE DEFAULT VALUE" %(src.conn,col, replaceString),"w")
                    tarType = tar.defDataType
                else:
                    targetList = tar.setDataTypeTree (dataTypeTree=newDataTypeTree, allDataTypes=tar.DATA_TYPES, ret=[])
                    tarType = '%s%s' %(targetList[-1],postType) if targetList and len(targetList)>0 else tar.defDatatType
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

    """ Check Source values in STT - remove invalid values """
    def __updateSTTBySource (self, srcStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        srcStrucureL = {x.replace(pre,"").replace(pos,"").lower():x for x in srcStructure}

        removeColumns = []
        if self.stt:
            for col in self.stt:
                if eJson.jSttValues.SOURCE in self.stt[col] and self.stt[col][eJson.jSttValues.SOURCE].replace(pre,"").replace(pos,"").lower() not in srcStrucureL:
                    removeColumns.append (col)

            for col in removeColumns:
                p("STT TAREGT %s HAVE INVALID SOURCE %s --> ignore COLUMN " % (col, self.stt[col][eJson.jSttValues.SOURCE]),"w")
                del self.stt[col]

    """ Check Taret values in STT - remove invalid values  """
    def __updateSTTByTarget (self, tarStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        tarStrucureL = {x.replace(pre,"").replace(pos,"").lower():x for x in tarStructure}
        if self.stt:
            for col in self.stt:
                if col.replace(pre,"").replace(pos,"").lower() not in tarStrucureL:
                    p("STT COLUMN %s NOT EXISTS OBJECT --> ignore COLUMN " %(col) ,"w")
                    #del self.stt[col]

    def dong(self):
        if self.nodes and len(self.nodes)>0:
            src         = None
            tar         = None
            mrg         = None
            mrgSource   = None
            for node in self.nodes:
                for k in node:
                    ## Exec METHOD
                    if k.isdigit():
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

    def ding(self):
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
                            tar.create(stt=self.stt)
                            tar.close()
                            tar = None

                    if tar and src:
                        # convert source data type to target data types
                        targetStt = self.updateTargetBySourceAndStt(src=src, tar=tar)
                        tar.create(stt=targetStt)
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
                        mrgTarget.create(stt=sttMerge)
                        mrgTarget.close()
                        mrgSource.close()
                        mrgSource = None