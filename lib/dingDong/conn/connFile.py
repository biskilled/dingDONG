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

import sys
import shutil
import os
import io
import time
import codecs
from collections import OrderedDict
if sys.version_info[0] == 2:
    import csv23 as csv
else:
    import csv as csv

from dingDONG.conn.baseConnBatch import baseConnBatch
from dingDONG.conn.transformMethods import  *
from dingDONG.misc.enums  import eConn, eJson, eObj
from dingDONG.misc.globalMethods import uniocdeStr, setProperty
from dingDONG.config import config
from dingDONG.misc.logger     import p

DEFAULTS = {
    eConn.defaults.FILE_MIN_SIZE :1024,
    eConn.defaults.FILE_DEF_COLUMN_PREF :'col_',
    eConn.defaults.FILE_ENCODING:'windows-1255',
    eConn.defaults.FILE_DELIMITER:',',
    eConn.defaults.FILE_ROW_HEADER:1,
    eConn.defaults.FILE_END_OF_LINE:'\r\n',
    eConn.defaults.FILE_MAX_LINES_PARSE:50000,
    eConn.defaults.FILE_LOAD_WITH_CHAR_ERR:'strict',
    eConn.defaults.FILE_APPEND:False,
    eConn.defaults.FILE_CSV:False,
    eConn.defaults.FILE_REPLACE_TO_NONE:None, #r'\"|\t',
    eConn.defaults.DEFAULT_TYPE:eConn.dataTypes.B_STR

    }

DATA_TYPES = {  }

class connFile (baseConnBatch):
    def __init__ (self, folder=None,fileName=None,
                    fileMinSize=None, colPref=None, encode=None,isCsv=None,
                    delimiter=None,header=None, endOfLine=None,linesToParse=None,
                    withCharErr=None,append=None,replaceToNone=None,
                    isTar=None, isSrc=None, propertyDict=None):

        self.connType = eConn.types.FILE
        baseConnBatch.__init__(self, connType=self.connType, propertyDict=propertyDict, defaults=DEFAULTS, isTar=isTar, isSrc=isSrc)

        """ FILE DEFAULTS PROPERTIES """

        self.fileMinSize= setProperty(k=eConn.defaults.FILE_MIN_SIZE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_MIN_SIZE], setVal=fileMinSize)
        self.colPref    = setProperty(k=eConn.defaults.FILE_DEF_COLUMN_PREF, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_DEF_COLUMN_PREF], setVal=colPref)
        self.encode     = setProperty(k=eConn.defaults.FILE_ENCODING, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_ENCODING], setVal=encode)
        self.header     = setProperty(k=eConn.defaults.FILE_ROW_HEADER, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_ROW_HEADER], setVal=header)
        self.maxLinesParse = setProperty(k=eConn.defaults.FILE_MAX_LINES_PARSE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_MAX_LINES_PARSE], setVal=linesToParse)
        self.withCharErr= setProperty(k=eConn.defaults.FILE_LOAD_WITH_CHAR_ERR, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_LOAD_WITH_CHAR_ERR], setVal=withCharErr)
        self.delimiter  = setProperty(k=eConn.defaults.FILE_DELIMITER, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_DELIMITER], setVal=delimiter)
        self.endOfLine  = setProperty(k=eConn.defaults.FILE_END_OF_LINE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_END_OF_LINE], setVal=endOfLine)
        self.append     = setProperty(k=eConn.defaults.FILE_APPEND, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_APPEND], setVal=append)
        self.replaceToNone= setProperty(k=eConn.defaults.FILE_REPLACE_TO_NONE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_REPLACE_TO_NONE], setVal=replaceToNone)
        self.isCsv      = setProperty(k=eConn.defaults.FILE_CSV, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.FILE_CSV], setVal=isCsv)
        self.defDataType= setProperty(k=eConn.defaults.DEFAULT_TYPE, o=self.propertyDict, defVal=DEFAULTS[eConn.defaults.DEFAULT_TYPE], setVal=isCsv)
        self.columnFrame= ('','')

        """ FILE PROPERTIES """
        self.fileFullName = None
        self.folder       = None
        self.fileName     = None
        self.folder   = setProperty(k=eConn.props.FOLDER, o=self.propertyDict, setVal=folder)

        if not self.folder:
            self.folder = setProperty(k=eConn.props.URL, o=self.propertyDict, defVal=None)

        self.fileName = setProperty(k=eConn.props.TBL, o=self.propertyDict, setVal=fileName)

        pre, pos = self.__getSplitedFileName(fullPath=self.fileName)
        if pre and pos:
            self.fileFullName   = self.fileName
            self.fileName       = pos
            self.folder         = pre

        elif self.folder and self.fileName:
            pre, pos = self.__getSplitedFileName(fullPath=os.path.join( self.folder, self.fileName ))
            if  pre and pos:
                self.fileFullName = os.path.join( self.folder, self.fileName )
                self.fileName = pos
                self.folder = pre

        else:
            pre, pos = self.__getSplitedFileName(fullPath=self.folder)
            if pre and pos:
                self.fileFullName   = self.folder
                self.fileName       = pos
                self.folder         = pre

        self.connect()

    def __getSplitedFileName (self, fullPath):
        pre, pos = None,None
        if fullPath:
            if os.path.isfile(fullPath):
                head, tail = os.path.split(fullPath)
                if head and len(head) > 0 and tail and len(tail) > 1:
                    pre = head
                    pos = os.path.splitext(tail)[0]
            elif os.path.isdir(fullPath):
                pre = fullPath
            elif self.connIsTar:
                head, tail = os.path.split(fullPath)
                if head and len(head) > 0 and tail and len(tail) > 1:
                    pre = head
                    pos = os.path.splitext(tail)[0]
        return pre,pos

    def connect(self, fileName=None):
        if self.fileFullName:
            self.objNames[self.fileName] = {eObj.FILE_FULL_PATH:self.fileFullName, eObj.FILE_FOLDER:self.folder}

            if os.path.isfile(self.fileFullName):
                p(u"FILE EXISTS:%s, DELIMITER %s, HEADER %s " % (self.fileFullName, self.delimiter, self.header), "ii")
            else:
                p(u"FILE NOT EXISTS:%s, DELIMITER %s, HEADER %s " % (self.fileFullName, self.delimiter, self.header), "ii")
        elif os.path.isdir (self.folder):
            self.isSingleObject = False
            for fileName in os.listdir(self.folder):
                fileFullPath = os.path.join(self.folder, fileName)
                pre, pos = self.__getSplitedFileName(fullPath=fileFullPath)

                if self.connFilter:
                    if fileName.split('.')[-1] == self.connFilter:
                        self.objNames[pos] = {eObj.FILE_FULL_PATH:fileFullPath, eObj.FILE_FOLDER:self.folder}
                        p(u"FILE IN FOLDER EXISTS:%s, DELIMITER %s, HEADER %s " % (fileName, self.delimiter, self.header), "ii")
                else:
                    self.objNames[pos] = {eObj.FILE_FULL_PATH: fileFullPath, eObj.FILE_FOLDER: self.folder}
                    p(u"FILE IN FOLDER EXISTS:%s, DELIMITER %s, HEADER %s " % (fileName, self.delimiter, self.header),"ii")


    def close(self):
        pass

    def test(self):
        baseConnBatch.test(self)

    def isExists(self, fullPath=None):
        fileDict = {'':{eObj.FILE_FULL_PATH: fullPath}} if fullPath else self.objNames

        for f in fileDict:
            if not os.path.isfile( fileDict[f][eObj.FILE_FULL_PATH] ):
                return False

        return True

    def create(self, sttDict=None, fullPath=None, addIndex=None):
        fileDict = {'': {eObj.FILE_FULL_PATH: fullPath}} if fullPath else self.objNames
        for ff in fileDict:
            stt = self.getStt(sttDict=sttDict, k=ff)
            fullPath = fileDict[ff][eObj.FILE_FULL_PATH]
            self.cloneObject(stt=stt, fullPath=fullPath, ffDict={ff:fileDict[ff]})

    """ INTERNAL USED: 
        for create method create new File is file is exist
        If config.TRACK_HISTORY will save old table as tablename_currentDate   """
    def cloneObject(self, stt=None, fullPath=None, ffDict=None):

        fileName = os.path.basename(fullPath)
        fileDir = os.path.dirname(fullPath)

        fileNameNoExtenseion = os.path.splitext(fileName)[0]
        fimeNameExtension = os.path.splitext(fileName)[1]
        ### check if table exists - if exists, create new table
        isFileExists = os.path.isfile(fullPath)
        toUpdateFile = False

        if isFileExists:
            actulSize = os.stat(fullPath).st_size
            if actulSize < self.fileMinSize:
                p("FILE %s EXISTS WITH SIZE SMALLER THAN %s --> WONT UPDATE  ..." % (fullPath, str(actulSize)), "ii")
                toUpdateFile = False

            fileStructure = self.getStructure(objects=ffDict)
            fileStructureL = [x.lower() for x in fileStructure]
            sttL = [x.lower() for x in stt]

            if set(fileStructureL) != set(sttL):
                toUpdateFile = True
                p("FILE %s EXISTS, SIZE %s STRUCTURE CHANGED !!" % (fullPath, str(actulSize)), "ii")
            else:
                p("FILE %s EXISTS, SIZE %s STRUCURE DID NOT CHANGED !! " % (fullPath, str(actulSize)), "ii")

            if toUpdateFile and config.DING_TRACK_OBJECT_HISTORY:
                oldName = None
                if (os.path.isfile(fullPath)):
                    oldName = fileNameNoExtenseion + "_" + str(time.strftime('%y%m%d')) + fimeNameExtension
                    oldName = os.path.join(fileDir, oldName)
                    if (os.path.isfile(oldName)):
                        num = 1
                        oldName = os.path.splitext(oldName)[0] + "_" + str(num) + os.path.splitext(oldName)[1]
                        oldName = os.path.join(fileDir, oldName)
                        while (os.path.isfile(oldName)):
                            num += 1
                            FileNoExt = os.path.splitext(oldName)[0]
                            FileExt = os.path.splitext(oldName)[1]
                            oldName = FileNoExt[: FileNoExt.rfind('_')] + "_" + str(num) + FileExt
                            oldName = os.path.join(fileDir, oldName)
                if oldName:
                    p("FILE HISTORY, FILE %s EXISTS, COPY FILE TO %s " % (str(self.fileName), str(oldName)), "ii")
                    shutil.copy(fullPath, oldName)

    def __getStructure  (self, fullPath):
        if not os.path.isfile( fullPath ):
            return None

        ret = OrderedDict()
        with io.open(fullPath, 'r', encoding=self.encode) as f:
            if not self.header:
                headers = f.readline().strip(self.endOfLine).split(self.delimiter)
                if headers and len(headers) > 0:
                    for i, col in enumerate(headers):
                        colStr = '%s%s' % (self.colPref, str(i))
                        ret[colStr] = {eJson.stt.TYPE: self.defDataType, eJson.stt.SOURCE: colStr}
            else:
                for i, line in enumerate(f):
                    if self.header - 1 == i:
                        headers = line.strip(self.endOfLine).split(self.delimiter)
                        if headers and len(headers) > 0:
                            for i, col in enumerate(headers):
                                ret[col] = {eJson.stt.TYPE: self.defDataType, eJson.stt.SOURCE: col}
                        break
        return ret

    """ Strucutre Dictinary for file: {Column Name: {ColumnType:XXXXX} .... }
        Types : STR , FLOAD , INT, DATETIME (only if defined)  """

    def getStructure (self, objects=None):
        objDict = objects if objects else self.objNames
        if not isinstance(objDict, (dict, OrderedDict)):
            if objects in self.objNames:
                return self.__getStructure(fullPath=self.objNames[objects][eObj.FILE_FULL_PATH] )
            else:
                p("FILE %s IS NOT EXISTS " %str(objects))
                return None

        if self.isSingleObject:
            return self.__getStructure(fullPath= self.fileFullName )
        else:
            retDicStructure = OrderedDict()
            for ff in objDict:
                retDicStructure[ff] = self.__getStructure(fullPath= objDict[ff][eObj.FILE_FULL_PATH] )

            return retDicStructure

    def preLoading(self, dictObj=None):
        if self.isExists() and self.append:
            p("FILE/S EXISTS WILL APPEND DATA " )

    def extract(self, tar, tarToSrcDict, batchRows=None):
        batchRows           = batchRows if batchRows else self.batchSize
        startFromRow        = 0 if not self.header else self.header
        fileStructureDict   = self.getStructure()

        if self.isSingleObject:
            if len(self.objNames)>0:
                fName = list(self.objNames.keys())[0]
                fileStructureDict = {fName:fileStructureDict}
                tarToSrcDict[fName] = tarToSrcDict['']
                del tarToSrcDict['']
            else:
                p("UNABLE TO EXTRACT FILE !!!")

        for fileName in fileStructureDict:
            fileStructure = fileStructureDict[fileName]
            fileFullPath    = self.objNames[fileName][eObj.FILE_FULL_PATH]
            fileStructureL  = OrderedDict()
            listOfColumnsH  = {}
            targetColumnList= []
            fnOnRowsDic     = {}
            execOnRowsDic   = {}
            listOfColumnsL  = []

            for i, col in enumerate(fileStructure):
                fileStructureL[col.lower()] = i
                listOfColumnsH[i] = col

            ## File with header and there is target to source mapping
            if tarToSrcDict and fileName in tarToSrcDict:
                tarToSrc = tarToSrcDict[fileName]
                mappingSourceColumnNotExists = u""
                fileSourceColumnNotExists    = u""

                for i, col in enumerate (tarToSrc):
                    targetColumnList.append(col)
                    if eJson.stt.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.stt.SOURCE]:
                        srcColumnName = tarToSrc[col][eJson.stt.SOURCE]
                        if srcColumnName.lower() in fileStructureL:
                            listOfColumnsL.append(fileStructureL[ srcColumnName.lower() ])
                        else:
                            mappingSourceColumnNotExists+=uniocdeStr (srcColumnName)+u" ; "
                    else:
                        listOfColumnsL.append(-1)

                    ### ADD FUNCTION
                    if eJson.stt.FUNCTION in tarToSrc[col] and tarToSrc[col][eJson.stt.FUNCTION]:
                        fnc = eval(tarToSrc[col][eJson.stt.FUNCTION])
                        fnOnRowsDic[ i ] = fnc if isinstance(fnc, (list, tuple)) else [fnc]

                    ### ADD EXECUTION FUNCTIONS
                    if eJson.stt.EXECFUNC in tarToSrc[col] and len(tarToSrc[col][eJson.stt.EXECFUNC]) > 0:
                        newExcecFunction = tarToSrc[col][eJson.stt.EXECFUNC]
                        regex = r"(\{.*?\})"
                        matches = re.finditer(regex, tarToSrc[col][eJson.stt.EXECFUNC], re.MULTILINE | re.DOTALL)
                        for matchNum, match in enumerate(matches):
                            for groupNum in range(0, len(match.groups())):
                                colName = match.group(1)
                                if colName and len(colName) > 0 and colName in fileStructureL:
                                    colName = colName.replace("{", "").replace("}", "")
                                    newExcecFunction.replace(colName, colName)
                        execOnRowsDic[i] = newExcecFunction

                for colNum in listOfColumnsH:
                    if colNum not in listOfColumnsL:
                        fileSourceColumnNotExists+= uniocdeStr( listOfColumnsH[colNum] )+u" ; "

                if len(mappingSourceColumnNotExists)>0:
                    p("SOURCE COLUMN EXISTS IN SOURCE TO TARGET MAPPING AND NOT FOUND IN SOURCE FILE: %s" %(mappingSourceColumnNotExists),"w")

                if len (fileSourceColumnNotExists)>0:
                    p("FILE COLUMN NOT FOUD IN MAPPING: %s" %(fileSourceColumnNotExists),"w")
            ## There is no target to source mapping, load file as is
            else:
                for colNum in listOfColumnsH:
                    listOfColumnsL.append(colNum)

            """ EXECUTING LOADING SOURCE FILE DATA """
            rows = []
            try:
                with io.open( fileFullPath, 'r', encoding=self.encode, errors=self.withCharErr) as textFile:
                    if self.isCsv:
                        fFile = csv.reader(textFile, delimiter=self.delimiter)
                        for i, split_line in enumerate(fFile):
                            if i>=startFromRow:
                                if self.replaceToNone:
                                    rows.append( [ re.sub(self.replaceToNone,"",split_line[x],re.IGNORECASE|re.MULTILINE|re.UNICODE)  if x>-1 and len(split_line[x])>0 else None for x in listOfColumnsL] )
                                else:
                                    rows.append([split_line[x] if x > -1 and len(split_line[x]) > 0 else None for x in listOfColumnsL])

                            if self.maxLinesParse and i>startFromRow and i%self.maxLinesParse == 0:
                                rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                                tar.load(rows=rows, targetColumn=targetColumnList, objectName=fileName)
                                rows = list ([])
                    else:
                        for i, line in enumerate(textFile):
                            line = re.sub(self.replaceToNone,"",line,re.IGNORECASE|re.MULTILINE|re.UNICODE) if self.replaceToNone else line
                            line = line.strip(self.endOfLine)
                            split_line = line.split(self.delimiter)
                            # Add headers structure
                            if i >= startFromRow:
                                rows.append([split_line[x] if x > -1 and len(split_line[x]) > 0 else None for x in listOfColumnsL])

                            if self.maxLinesParse and i > startFromRow and i % self.maxLinesParse == 0:
                                rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                                tar.load(rows=rows, targetColumn=targetColumnList, objectName=fileName)
                                rows = list([])

                    if len(rows)>0 : #and split_line:
                        rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)

                        tar.load(rows=rows, targetColumn=targetColumnList, objectName=fileName)
                        rows = list ([])

            except Exception as e:
                p("ERROR LOADING FILE %s  >>>>>>" % (fileFullPath) , "e")
                p(str(e), "e")

    def load(self, rows, targetColumn, objectName=None):
        totalRows = len(rows) if rows else 0
        if totalRows == 0:
            p("THERE ARE NO ROWS","w")
            return

        if self.append:
            pass

        fileName = self.fileFullName
        if objectName and len(objectName)>0:
            if objectName in self.objNames:
                fileName= self.objNames[objectName][eObj.FILE_FULL_PATH]
            else:
                p("FILE %s IS NOT EXISTS !!" %(str(objectName)) ,  "e")
                return

        with codecs.open(filename=fileName, mode='wb', encoding=self.encode) as f:
            if targetColumn and len(targetColumn) > 0:
                f.write(self.delimiter.join(targetColumn))
                f.write(self.endOfLine)

            for row in rows:
                row = [str(s) for s in row]
                f.write(self.delimiter.join(row))
                f.write(self.endOfLine)

        p('LOAD %s ROWS INTO FILE %s >>>>>> ' % (str(totalRows), self.fileFullName), "ii")
        return

    def execMethod(self, method=None):
        pass

    def merge (self, fileName, hearKeys=None, sourceFile=None):
        raise NotImplementedError("Merge need to be implemented")

    def cntRows (self, objName=None):
        raise NotImplementedError("count rows need to be implemented")

    def createFromDbStrucure(self, stt=None, objName=None, addIndex=None):
        raise NotImplementedError("createFrom need to be implemented")
