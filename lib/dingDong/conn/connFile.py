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

import shutil
import re
import os
import io
import time
import codecs
import csv
from collections import OrderedDict

from lib.dingDong.conn.baseBatch  import baseBatch
from lib.dingDong.misc.enumsJson  import eConn, eJson
from lib.dingDong.config          import config
from lib.dingDong.misc.logger     import p

DEFAULTS = {
            eJson.jFile.MIN_SIZE:1024,
            eJson.jFile.DEF_COLUMN_PREF :'col_',
            eJson.jFile.DECODING:"windows-1255",
            eJson.jFile.ENCODING:'utf8',
            eJson.jFile.DELIMITER:',',
            eJson.jFile.ROW_HEADER:1,
            eJson.jFile.END_OF_LINE:'\r\n',
            eJson.jFile.MAX_LINES_PARSE:10000,
            eJson.jFile.LOAD_WITH_CHAR_ERR:'strict',
            eJson.jFile.APPEND:False
           }

DATA_TYPES = {  }

class connFile (baseBatch):

    def __init__ (self, folder=None,fileName=None,
                    fileMinSize=512, colPref='col_', decode="windows-1255", encode='utf8',
                    delimiter=',',header=1, endOfLine='\r\n',linesToParse=10000,withCharErr='strict',append=False,
                    isTar=False, isSrc=False, connPropDict=None):

        self.conn = eConn.FILE
        baseBatch.__init__(self, conn=self.conn, connPropDict=connPropDict)
        self.DEFAULTS   = DEFAULTS
        self.DATA_TYPES = DATA_TYPES
        self.usingSchema = False

        """ BASIC PROPERTIES FROM BASECONN """
        self.connName       = self.connName
        self.defDataType    = self.defDataType

        """ FILE DEFAULTS PROPERTIES """

        self.fileMinSize= self.setProperties (propKey=eJson.jFile.MIN_SIZE, propVal=fileMinSize, propDef=DEFAULTS)
        self.colPref    = self.setProperties(propKey=eJson.jFile.DEF_COLUMN_PREF, propVal=colPref, propDef=DEFAULTS)
        self.decode     = self.setProperties(propKey=eJson.jFile.DECODING, propVal=decode, propDef=DEFAULTS)
        self.encode     = self.setProperties(propKey=eJson.jFile.ENCODING, propVal=encode, propDef=DEFAULTS)
        self.header     = self.setProperties(propKey=eJson.jFile.ROW_HEADER, propVal=header, propDef=DEFAULTS)
        self.maxLinesParse = self.setProperties(propKey=eJson.jFile.MAX_LINES_PARSE, propVal=linesToParse, propDef=DEFAULTS)
        self.withCharErr= self.setProperties(propKey=eJson.jFile.LOAD_WITH_CHAR_ERR, propVal=withCharErr, propDef=DEFAULTS)
        self.delimiter  = self.setProperties(propKey=eJson.jFile.DELIMITER, propVal=delimiter, propDef=DEFAULTS)
        self.endOfLine  = self.setProperties(propKey=eJson.jFile.END_OF_LINE, propVal=endOfLine, propDef=DEFAULTS)
        self.append     = self.setProperties(propKey=eJson.jFile.APPEND, propVal=append, propDef=DEFAULTS)

        """ FILE PROPERTIES """
        self.fileFullName = None
        self.folder   = self.setProperties(propKey=eJson.jValues.FOLDER, propVal=folder)
        if not self.folder:
            self.folder = self.setProperties(propKey=eJson.jValues.URL, propVal=folder)

        self.fileName = self.setProperties(propKey=eJson.jValues.OBJ, propVal=fileName)
        self.isSrc    = self.setProperties(propKey=eJson.jValues.IS_SOURCE, propVal=isSrc)
        self.isTar    = self.setProperties(propKey=eJson.jValues.IS_TARGET, propVal=isTar)

        if self.fileName:
            head, tail = os.path.split (self.fileName)
            if head and len(head)>0 and tail and len (tail)>1:
                self.fileFullName   = self.fileName
                self.folder         = head
            elif self.folder:
                self.fileFullName = os.path.join(self.folder, self.fileName)
            else:
                p("THERE IS NO FOLDER MAPPING, FILE CONNENTION FAILED, %s" %(self.fileName), "e")
                return
            if (os.path.isfile(self.fileFullName)):
                p ("FILE EXISTS:%s, DELIMITER %s, HEADER %s " %(str(self.fileFullName) , str(self.delimiter) ,str(self.header) ), "ii")
        elif self.folder:
            head, tail = os.path.split(self.folder)
            if head and len(head)>0 and tail and len (tail)>1:
                self.folder = head
                self.fileFullName = self.folder

    """  MANADATORY METHOD INHERTIED FROM BASE BATCH INTERFACE"""
    def connect(self, fileName=None):
        if fileName:
            self.fileFullName = fileName
            return True
        elif not self.fileFullName:
            if self.folder and os.path.isdir(self.folder):
                p("CONNETCTED USING FOLDER %s" %self.folder)
                return True
            else:
                err = u"FILE NOT VALID: %s" %(self.decodeStrPython2Or3(sObj=self.fileFullName, un=True, decode=self.decode))
                raise ValueError(err)
        return True

    def close(self):
        pass

    def create(self, stt=None, fullPath=None):
        fullPath = fullPath if fullPath else self.fileFullName
        self.cloneObject(stt=stt, fullPath=fullPath)
        p("NO POINT TO CREATE FILE %s " %(fullPath), "ii")

    """  Strucutre Dictinary for file: {Column Name: {ColumnType:XXXXX} .... }
            Types : STR , FLOAD , INT, DATETIME (only if defined)  """
    def getStructure(self, fullPath=None):
        ret     = OrderedDict()
        fullPath= fullPath if fullPath else self.fileFullName

        if self.isExists(fullPath=fullPath):
            with io.open(fullPath, 'r', encoding=self.decode) as f:
                if not self.header:
                    headers = f.readline().strip(self.endOfLine).split(self.delimiter)
                    if headers and len(headers) > 0:
                        for i, col in enumerate(headers):
                            colStr = '%s%s' %(self.colPref, str(i))
                            ret[colStr] = {eJson.jStrucure.TYPE: self.defDataType, eJson.jStrucure.ALIACE: None}
                else:
                    for i, line in enumerate(f):
                        if self.header-1 == i:
                            headers = line.strip(self.endOfLine).split(self.delimiter)
                            if headers and len(headers) > 0:
                                for i, col in enumerate(headers):
                                    ret[col] = {eJson.jStrucure.TYPE: self.defDataType, eJson.jStrucure.ALIACE: None}

                            break
        else:
            p ('FILE NOT EXISTS %s >>> ' %( str(fullPath) ), "ii")
        return ret

    def isExists(self, fullPath=None):
        fullPath = fullPath if fullPath else self.fileFullName
        return os.path.isfile(fullPath)

    def preLoading(self):
        if self.isExists() and self.append:
            p("FILE %s EXISTS WILL APPEND DATA " % (self.fileFullName))

    def extract(self, tar, tarToSrc, batchRows, addAsTaret=True):
        fnOnRowsDic     = {}
        execOnRowsDic   = {}
        loadFileAsIs    = False

        startFromRow    = 0 if not self.header else self.header
        listOfColumnsH  = {}
        listOfColumnsL  = []
        targetColumnList= []

        fileStructure = self.getStructure()
        fileStructureL = OrderedDict()

        for i, col in enumerate(fileStructure):
            fileStructureL[col.lower()] = i
            listOfColumnsH[i] = col

        ## File with header and there is target to source mapping
        if tarToSrc and len(tarToSrc)>0:
            mappingSourceColumnNotExists = []
            fileSourceColumnNotExists    = []

            for i, col in enumerate (tarToSrc):
                targetColumnList.append(col)
                if eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE]:
                    srcColumnName = tarToSrc[col][eJson.jSttValues.SOURCE]
                    if srcColumnName.lower() in fileStructureL:
                        listOfColumnsL.append(fileStructureL[ srcColumnName.lower() ])
                    else:
                        mappingSourceColumnNotExists.append (srcColumnName)
                else:
                    listOfColumnsL.append(None)

                ### ADD FUNCTION
                if eJson.jSttValues.FUNCTION in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.FUNCTION]:
                    fnc = eval(tarToSrc[col][eJson.jSttValues.FUNCTION])
                    fnOnRowsDic[ i ] = fnc if isinstance(fnc, (list, tuple)) else [fnc]

                ### ADD EXECUTION FUNCTIONS
                if eJson.jSttValues.EXECFUNC in tarToSrc[col] and len(tarToSrc[col][eJson.jSttValues.EXECFUNC]) > 0:
                    newExcecFunction = tarToSrc[col][eJson.jSttValues.EXECFUNC]
                    regex = r"(\{.*?\})"
                    matches = re.finditer(regex, tarToSrc[col][eJson.jSttValues.EXECFUNC], re.MULTILINE | re.DOTALL)
                    for matchNum, match in enumerate(matches):
                        for groupNum in range(0, len(match.groups())):
                            colName = match.group(1)
                            if colName and len(colName) > 0 and colName in fileStructureL:
                                colName = colName.replace("{", "").replace("}", "")
                                newExcecFunction.replace(colName, colName)
                    execOnRowsDic[i] = newExcecFunction

            for colNum in listOfColumnsH:
                if colNum not in listOfColumnsL:
                    fileSourceColumnNotExists.append ( listOfColumnsH[colNum] )

            if len(mappingSourceColumnNotExists)>0:
                p("SOURCE COLUMN EXISTS IN SOURCE TO TARGET MAPPING AND NOT FOUND IN SOURCE FILE: %s" %(str(mappingSourceColumnNotExists)),"e")

            if len (fileSourceColumnNotExists)>0:
                p("FILE COLUMN NOT FOUD IN MAPPING: %s" %(str(fileSourceColumnNotExists)),"ii")
        ## There is no target to source mapping, load file as is
        else:
            for colNum in listOfColumnsH:
                listOfColumnsL.append(colNum)

        if None not in listOfColumnsL and set(listOfColumnsL) == set(listOfColumnsH):
            loadFileAsIs = True


        """ EXECUTING LOADING SOURCE FILE DATA """
        rows = []
        try:
            with io.open (self.fileFullName, 'r', encoding=self.encode, errors=self.withCharErr) as textFile:
                fFile = csv.reader(textFile, delimiter=self.delimiter)
                for i, split_line in enumerate(fFile):
                    #line = line.replace('"', '').replace("\t", "")
                    #line = line.strip(self.endOfLine)
                    #split_line = line.split(self.delimiter)
                    # Add headers structure
                    if i>=startFromRow:
                        if loadFileAsIs:
                            rows.append(split_line)
                        else:
                            rows.append( [split_line[x] if x else None for x in listOfColumnsL] )

                    if self.maxLinesParse and i>startFromRow and i%self.maxLinesParse == 0:
                        rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                        tar.load(rows=rows, targetColumn=targetColumnList)
                        rows = list ([])

                if len(rows)>0 : #and split_line:
                    rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                    tar.load(rows=rows, targetColumn=targetColumnList)
                    rows = list ([])

        except Exception as e:
            p("ERROR LOADING FILE %s  >>>>>>" % (self.fileFullName) , "e")
            p(str(e), "e")

    def load(self, rows, targetColumn):
        totalRows = len(rows) if rows else 0
        if totalRows == 0:
            p("THERE ARE NO ROWS")
            return

        if self.append:
            pass

        with codecs.open(filename=self.fileFullName, mode='wb', encoding=self.encode) as f:
            if targetColumn and len(targetColumn) > 0:
                f.write(self.delimiter.join(targetColumn))
                f.write(self.endOfLine)

            for row in rows:
                row = [str(s) for s in row]
                f.write(self.delimiter.join(row))
                f.write(self.endOfLine)



        p('LOAD %s ROWS INTO FILE %s >>>>>> ' % (str(totalRows), self.fileFullName), "ii")
        return

    def execMethod(self):
        pass

    """ PUBLIC METHOD FOR DB MANIPULATION  """

    """ INTERNAL USED for create method
        Create new File is file is exist
        If config.TRACK_HISTORY will save old table as tablename_currentDate   """
    def cloneObject(self, stt=None, fullPath=None):
        fullPath    = fullPath if fullPath else self.fileFullName
        fileName    = os.path.basename(fullPath)
        fileDir     = os.path.dirname(fullPath)

        fileNameNoExtenseion = os.path.splitext(fileName)[0]
        fimeNameExtension = os.path.splitext(fileName)[1]
        ### check if table exists - if exists, create new table
        isFileExists = os.path.isfile(fullPath)
        toUpdateFile = False

        if isFileExists:
            actulSize = os.stat(fullPath).st_size
            if actulSize < self.fileMinSize:
                p("FILE %s EXISTS WITH SIZE SMALLER THAN %s --> WONT UPDATE  ..." %(fullPath, str(actulSize)), "ii")
                toUpdateFile = False

            fileStructure = self.getStructure(fullPath=fullPath)
            fileStructureL= [x.lower() for x in fileStructure]
            sttL = [x.lower() for x in stt]

            if set(fileStructureL) != set(sttL):
                toUpdateFile = True
                p("FILE %s EXISTS, SIZE %s STRUCTURE CHANGED !!" % (fullPath, str(actulSize)), "ii")
            else:
                p("FILE %s EXISTS, SIZE %s STRUCURE DID NOT CHANGED !! " % (fullPath, str(actulSize)), "ii")






            if toUpdateFile and config.TRACK_HISTORY:
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

    """ INTERNAL METHOD  """
    def merge (self, fileName, hearKeys=None, sourceFile=None):
        raise NotImplementedError("count rows need to be implemented")