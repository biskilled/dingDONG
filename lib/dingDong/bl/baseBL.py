import re
from collections import OrderedDict

from dingDong.misc.logger       import p
from dingDong.bl.jsonParser     import jsonParser
from dingDong.misc.enumsJson    import eJson
from dingDong.conn.baseConnectorManager   import mngConnectors as conn

class baseBL:
    def __init__ (self, name='base model', dicObj=None, filePath=None,
                dirData=None, includeFiles=None, notIncludeFiles=None,
                connDict=None):
        self.name       = name
        self.jsonParser = jsonParser(dicObj=dicObj, filePath=filePath,
                                     dirData=dirData, includeFiles=includeFiles, notIncludeFiles=notIncludeFiles,
                                     connDict=connDict)

        ## Defualt properties
        self.stt            = None
        self.addSourceColumn= True
        self.src            = None
        self.tar            = None
        self.mrg            = None
        self.exec           = None

        self.mergeSource    = None
        self.mergeTarget    = None
        self.mergeKeys      = None

    def setSTT (self, node):

        if eJson.jKeys.STTONLY in node:
            self.stt            = node[eJson.jKeys.STTONLY]
            self.addSourceColumn= False

        elif eJson.jKeys.STT in node:
            self.stt = node[eJson.jKeys.STT]
        else:
            self.stt            = {}
            self.addSourceColumn= True

    def setMainProperty (self,key, valDict):
        if eJson.jKeys.EXEC == key or eJson.jKeys.EXEC in valDict:
            self.exec = valDict

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

    def model (self):

        p('baseBL->blExec: START %s >>>>>' %(self.name), "i")
        self.execJsonAllNodes()

        p('baseBL->blExec: FINISH %s >>>>>' % (self.name), "i")
        p('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', "ii")

    def execJsonAllNodes (self):
        for jsonMapping in self.jsonParser.jsonMappings(destList=None):
            for jsonFile in jsonMapping:
                self.execJsonNode(jsName=jsonFile,jsonNodes=jsonMapping[jsonFile])

    def execJsonNode (self, jsName, jsonNodes):
        pass

    """ Check Source values in STT - remove invalid values """
    def __updateSTTBySource (self, srcStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        srcStrucureL = {x.replace(pre,"").replace(pos,"").lower():x for x in srcStructure}
        for col in self.stt:
            if eJson.jSttValues.SOURCE in self.stt[col] and self.stt[col][eJson.jSttValues.SOURCE].replace(pre,"").replace(pos,"").lower() not in srcStrucureL:
                p("STT TAREGT %s HAVE INVALID SOURCE %s --> ignore COLUMN " %(col,  self.stt[col][eJson.jSttValues.SOURCE]) ,"e")
                del self.stt[col]

    """ Check Taret values in STT - remove invalid values  """
    def __updateSTTByTarget (self, tarStructure, pre="[", pos="]"):
        # Check if ther are sourcea in STT that not defined
        tarStrucureL = {x.replace(pre,"").replace(pos,"").lower():x for x in tarStructure}
        for col in self.stt:
            if col.replace(pre,"").replace(pos,"").lower() not in tarStrucureL:
                p("STT COLUMN %s NOT EXISTS OBJECT --> ignore COLUMN " %(col) ,"e")
                #del self.stt[col]

    """ LOADING MODULE: Return mapping between source and target """
    def mappingLoadingSourceToTarget (self, src, tar):
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
            colAlias= srcStructure[col][eJson.jStrucure.ALIACE] if eJson.jStrucure.ALIACE in srcStructure[col] else None
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
    def updateTargetBySourceAndStt(self, src, tar):
        retStrucure = OrderedDict()
        sourceStt   = src.getStructure()

        sourceColL  = {x.lower():x for x in sourceStt}

        if src.conn == tar.conn:
            if self.addSourceColumn:
                for col in sourceStt:
                    if eJson.jStrucure.ALIACE in sourceStt[col] and sourceStt[col][eJson.jStrucure.ALIACE]:
                        retStrucure[ sourceStt[col][eJson.jStrucure.ALIACE] ] = {eJson.jStrucure.TYPE: sourceStt[col][eJson.jStrucure.TYPE]}
                    else:
                        retStrucure[col] = {eJson.jStrucure.TYPE: sourceStt[col][eJson.jStrucure.TYPE]}

        ### Source connection type is different than target connection type
        elif self.addSourceColumn:
            for col in sourceStt:
                targetColName = col
                if eJson.jStrucure.ALIACE in sourceStt[col] and sourceStt[col][eJson.jStrucure.ALIACE]:
                    targetColName = sourceStt[col][eJson.jStrucure.ALIACE]

                colType = sourceStt[col][eJson.jStrucure.TYPE] if eJson.jStrucure.TYPE in sourceStt[col] and sourceStt[col][eJson.jStrucure.TYPE] else tar.defDataType
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
                    tarType = tar.defDatatType
                else:
                    targetList = tar.setDataTypeTree (dataTypeTree=newDataTypeTree, allDataTypes=tar.dataTypes, ret=[])
                    tarType = '%s%s' %(targetList[-1],postType) if targetList and len(targetList)>0 else tar.defDatatType
                retStrucure[targetColName] = {eJson.jStrucure.TYPE:tarType}

        for col in self.stt:
            if col.lower() in sourceColL and eJson.jSttValues.TYPE in self.stt[col] and self.stt[col][eJson.jSttValues.TYPE]:
                retStrucure[sourceColL[ col.lower() ]][eJson.jSttValues.TYPE] = self.stt[col][eJson.jSttValues.TYPE]

                if eJson.jSttValues.ALIACE in self.stt[col]:
                    retStrucure[sourceColL[col.lower()]][eJson.jSttValues.ALIACE] = self.stt[col][
                        eJson.jSttValues.ALIACE]

            elif col.lower() not in sourceColL:
                if eJson.jSttValues.TYPE in self.stt[col] and self.stt[col][eJson.jSttValues.TYPE]:
                    retStrucure[col] = {eJson.jStrucure.TYPE: self.stt[col][eJson.jSttValues.TYPE]}
                else:
                    retStrucure[col] = {eJson.jStrucure.TYPE: self.tar.defDataType}

                    if eJson.jSttValues.ALIACE in self.stt[col]:
                        retStrucure[col][eJson.jSttValues.ALIACE] = self.stt[col][eJson.jSttValues.ALIACE]

        return retStrucure

    """ LOADING BUSINESS LOGIC EXECUTOERS """
    def dbExec (self,  queries):
        pass

