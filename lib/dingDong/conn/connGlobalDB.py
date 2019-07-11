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

import abc
import re
import os
import sys
import time
from collections import OrderedDict

from dingDong.conn.baseBatch            import baseBatch
from dingDong.conn.baseBatchFunction    import *
from dingDong.misc.enumsJson            import eConn, eJson, eSql
from dingDong.misc.misc                 import decodePython2Or3
from dingDong.config                    import config
from dingDong.misc.logger               import p
import dingDong.conn.connGlobalDbQueryParser as qp
from dingDong.conn.connGlobalDbSqlQueries import setSqlQuery

DEFAULTS = {
            eConn.NONO: {   eJson.jValues.DEFAULT_TYPE:'varchar(100)',eJson.jValues.SCHEMA:'dbo',
                            eJson.jValues.EMPTY:'Null',eJson.jValues.COLFRAME:("[","]"),
                            eJson.jValues.SP:{'match':None, 'replace':None}},

            eConn.ORACLE: {eJson.jValues.DEFAULT_TYPE: 'varchar(100)', eJson.jValues.SCHEMA: 'dbo',
                           eJson.jValues.EMPTY: 'Null', eJson.jValues.COLFRAME: ("[", "]"),
                           eJson.jValues.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}},

            eConn.SQLSERVER: {eJson.jValues.DEFAULT_TYPE: 'varchar(100)', eJson.jValues.SCHEMA: 'dbo',
                              eJson.jValues.EMPTY: 'Null', eJson.jValues.COLFRAME: ("[", "]"),
                              eJson.jValues.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}  },

            eConn.LITE: {   eJson.jValues.DEFAULT_TYPE:'varchar(100)',eJson.jValues.SCHEMA:None,
                            eJson.jValues.EMPTY:'Null'}
           }

DATA_TYPES = {
    eConn.ORACLE:   { eConn.dataType.DB_DATE:['date','datetime'],
                        eConn.dataType.DB_VARCHAR:['varchar','varchar2']},
    eConn.SQLSERVER: {
                        eConn.dataType.DB_DATE:['smalldatetime','datetime']},

    eConn.ACCESS: { eConn.dataType.DB_VARCHAR:['varchar', 'longchar', 'bit', 'ntext'],
                    eConn.dataType.DB_INT:['integer', 'counter'],
                    eConn.dataType.DB_FLOAT:['double'],
                    eConn.dataType.DB_DECIMAL:['decimal']
                    }
}

class baseGlobalDb (baseBatch):

    def __init__ (self, conn=None, connUrl=None, connExtraUrl=None,
                  connName=None,connObj=None, connPropDict=None, connFilter=None, connIsTar=None,
                  connIsSrc=None, connIsSql=None):

        baseBatch.__init__(self, conn=conn, connName=connName, connPropDict=connPropDict)

        self.DEFAULTS   = DEFAULTS
        self.DATA_TYPES = DATA_TYPES
        self.usingSchema= True

        """ BASIC PROPERTIES FROM BASECONN """
        self.conn           = self.conn
        self.connName       = self.connName
        self.defDataType    = self.defDataType

        """ DB PROPERTIES """
        self.connUrl = self.setProperties(propKey=eJson.jValues.URL, propVal=connUrl)
        self.connExtraUrl = self.setProperties(propKey=eJson.jValues.URLPARAM, propVal=connExtraUrl)

        self.connObj    = self.setProperties (propKey=eJson.jValues.OBJ, propVal=connObj)
        self.connFilter = self.setProperties (propKey=eJson.jValues.FILTER, propVal=connFilter)
        self.connIsSrc  = self.setProperties (propKey=eJson.jValues.IS_SOURCE, propVal=connIsSrc)
        self.connIsTar  = self.setProperties (propKey=eJson.jValues.IS_TARGET, propVal=connIsTar)
        self.connIsSql  = self.setProperties (propKey=eJson.jValues.IS_SQL, propVal=connIsSql)

        self.defaultSchema  = self.defaults[eJson.jValues.SCHEMA]
        self.defaulNull     = self.defaults[eJson.jValues.EMPTY]
        self.defaultSP      = self.defaults[eJson.jValues.SP]
        self.columnFrame    = self.defaults[eJson.jValues.COLFRAME]

        self.cursor         = None
        self.connDB         = None
        self.connSql        = None

        self.parrallelProcessing    = False

        if not self.connUrl:
            err = "baseConn->init: Connection %s, NAME %s, must have VALID URL ! " %(self.conn, self.connName)
            raise ValueError(err)

        if self.connIsSql:
            self.connSql    = self.setQueryWithParams(self.connObj)
            self.connObj    = self.connSql

        elif self.connObj and len(self.connObj)>0:
            self.connSql = "SELECT * FROM %s" %self.connObj

            self.connObj        = self.wrapColName(col=self.connObj, remove=True).split(".")
            self.defaultSchema  = self.connObj[0] if len(self.connObj) > 1 else self.defaultSchema
            self.connObj        = self.connObj[1] if len(self.connObj) > 1 else self.connObj[0]

            if self.connFilter and len(self.connFilter) > 1:
                self.connFilter = re.sub(r'WHERE', '', self.connFilter, flags=re.IGNORECASE)
                self.connSql = '%s WHERE %s' %(self.connSql, self.setQueryWithParams(self.connFilter))


        objName = "QUERY " if self.connIsSql else "TABLE: %s" %self.connObj

        self.connect()
        p("CONNECTED, DB TYPE: %s, %s" % (self.conn, objName, ), "ii")

    """  MANADATORY METHOD INHERTIED FROM BASE BATCH INTERFACE"""

    def connect(self):
        try:
            if eConn.MYSQL == self.conn:
                import pymysql
                self.connDB = pymysql.connect(self.connUrl["host"], self.connUrl["user"], self.connUrl["passwd"],
                                              self.connUrl["db"])
                self.cursor = self.connDB.cursor()
            elif eConn.VERTICA == self.conn:
                import vertica_python
                self.connDB = vertica_python.connect(self.connUrl)
                self.cursor = self.connDB.cursor()
            elif eConn.ORACLE == self.conn:
                import cx_Oracle
                self.connDB = cx_Oracle.connect(self.connUrl['user'], self.connUrl['pass'], self.connUrl['dsn'])
                if 'nls' in self.connUrl:
                    os.environ["NLS_LANG"] = self.connUrl['nls']
                self.cursor = self.connDB.cursor()
            elif eConn.ACCESS == self.conn:
                import pyodbc as odbc
                self.connDB = odbc.connect(self.connUrl)  # , ansi=True
                self.cursor = self.conn.cursor()
                self.cColoumnAs = False
            elif eConn.LITE == self.conn:
                import sqlite3 as sqlite
                self.connDB = sqlite.connect(self.connUrl)  # , ansi=True
                self.cursor = self.connDB.cursor()
            else:
                try:
                    import ceODBC as odbc
                except ImportError:
                    # p("ceODBC is not installed will try to load pyodbc", "ii")
                    try:
                        import pyodbc as odbc
                    except ImportError:
                        p("pyobbc is not installed", "ii")
                if odbc:
                    self.connDB = odbc.connect(self.connUrl)  # ansi=True
                    self.cursor = self.connDB.cursor()
            return True
        except ImportError:
            p("%s is not installed" % (self.conn), "e")
        except Exception as e:
            err = "Error connecting into DB: %s, ERROR: %s " % (self.conn, str(e))
            raise ValueError(err)

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connDB:
                self.connDB.close()
            self.connDB   = None
            self.cursor = None
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            p("baseConnDb->Close: ERROR: file name:"+str(fname)+" line: "+str(exc_tb.tb_lineno)+" massage: "+str(exc_obj), "e")

    def create(self, stt=None, objName=None):
        tableSchema, tableName = self.setTableAndSchema(tableName=objName, tableSchema=None, wrapTable=False)
        tableFullName = '%s.%s'%(tableSchema, tableName) if tableSchema else tableName

        if not stt or len(stt) == 0:
            p("baseConnDb->create: TABLE %s NOT MAPPED CORRECLTY " %(self.connObj), "e")
            return
        boolToCreate = self.cloneObject(stt, tableSchema, tableName)

        if boolToCreate:
            sql = "CREATE TABLE %s \n (" %(tableFullName)
            for col in stt:
                if eJson.jStrucure.ALIACE in stt[col] and stt[col][eJson.jStrucure.ALIACE] and len (stt[col][eJson.jStrucure.ALIACE])>0:
                    colName = self.wrapColName (col=stt[col][eJson.jStrucure.ALIACE], remove=False)
                else:
                    colName =  self.wrapColName (col=col, remove=False)
                colType =  stt[col][eJson.jStrucure.TYPE]
                sql += '\t%s\t%s,\n' %(colName, colType)
            sql = sql[:-2]+')'


            p("baseConnDb->create: CREATE TABLE: \n" + sql)
            self.exeSQL(sql=sql, commit=True)

    """ INTERFACE: baseConn, baseConnDB, IMPLEMENTED: db, Method:getStrucutre - return Structure dictionary 
        Strucutre Dictinary: {Column Name: {ColumnType:XXXXX, ColumnAliace: NewColumns } .... } 
        Help methods: __getAccessStructure (db), getQueryStructure (baseConnDB), getDBStructure (baseConnDB)"""
    def getStructure(self, tableName=None, tableSchema=None, sqlQuery=None):
        sqlQuery = sqlQuery if sqlQuery else self.connSql
        tableSchema, tableName = self.setTableAndSchema (tableName=tableName, tableSchema=tableSchema)
        retStructure = None
        pre,pos = self.columnFrame[0],self.columnFrame[1]

        # If there is query and there is internal maaping in query - will add this mapping to mappingColum dictionary
        if self.connIsSql:
            retStructure = self.getQueryStructure(sqlQuery=sqlQuery)

        elif tableName and len(tableName) > 0:
            retStructure = self.getDBStructure(tableSchema=tableSchema, tableName=tableName)

        finallStructure = OrderedDict()
        for col in  retStructure:
            col = col.replace (pre,"").replace(pos,"")
            finallStructure[col] = retStructure[col]

            if eJson.jStrucure.ALIACE in finallStructure[col] and finallStructure[col][eJson.jStrucure.ALIACE] is not None:
                finallStructure[col][eJson.jStrucure.ALIACE] = finallStructure[col][eJson.jStrucure.ALIACE].replace(pre,"").replace(pos,"")

        return finallStructure

    def isExists(self, tableName, tableSchema):
        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.ISEXISTS, tableName=tableName, tableSchema=tableSchema)
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        if row[0]:
            p("baseConnDb->isExists SCHEMA:%s, TABLE:%s EXISTS" %(tableSchema, tableName), "ii")
            return True
        p("baseConnDb->isExists: SCHEMA:%s, TABLE:%s NOT EXISTS" % (tableSchema, tableName), "ii")
        return False

    def preLoading(self, tableName=None, tableSchema=None, sqlFilter=None):
        sqlFilter = sqlFilter if sqlFilter else self.connFilter
        if sqlFilter and len(sqlFilter) > 0:
            self.delete(sqlFilter=sqlFilter, tableName=tableName, tableSchema=tableSchema)
        else:
            self.truncate(tableName=tableName, tableSchema=tableSchema)

    def extract(self, tar, tarToSrc, batchRows, addAsTaret=True):
        fnOnRowsDic     = {}
        execOnRowsDic   = {}
        pre,pos         = self.columnFrame[0],self.columnFrame[1]
        sourceColumnStr = []
        targetColumnStr = []
        sourceSql       = self.connSql

        existingColumns = qp.existsColumnInQuery (sqlStr=sourceSql, pre=pre, pos=pos)

        ## There is Source And Target column mapping
        if tarToSrc and len (tarToSrc)>0:
            for i,col in  enumerate (tarToSrc):
                colTarName = '%s%s%s' % (pre, col.replace(pre, "").replace(pos, ""), pos)

                if eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE]:
                    srcNameL = tarToSrc[col][eJson.jSttValues.SOURCE].replace(pre,"").replace(pos,"").lower()
                    if srcNameL in existingColumns:
                        colSrcName = existingColumns[ srcNameL ]
                    else:
                        colSrcName = '%s%s%s' % (pre, tarToSrc[col][eJson.jSttValues.SOURCE].replace(pre, "").replace(pos, ""), pos)
                        colSrcName = '%s As %s' %(colSrcName, colTarName) if addAsTaret else colSrcName
                else:
                    colSrcName =  "'' As %s" %(colTarName) if addAsTaret else ''

                sourceColumnStr.append (colSrcName)
                targetColumnStr.append (col)

                ### ADD FUNCTION
                if eJson.jSttValues.FUNCTION in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.FUNCTION]:
                    fnc = eval(tarToSrc[col][eJson.jSttValues.FUNCTION])
                    fnOnRowsDic[i] = fnc if isinstance(fnc, (list, tuple)) else [fnc]

                ### ADD EXECUTION FUNCTIONS
                elif eJson.jSttValues.EXECFUNC in tarToSrc[col] and len(tarToSrc[col][eJson.jSttValues.EXECFUNC])>0:
                    newExcecFunction = tarToSrc[col][eJson.jSttValues.EXECFUNC]
                    regex   = r"(\{.*?\})"
                    matches = re.finditer(regex, tarToSrc[col][eJson.jSttValues.EXECFUNC], re.MULTILINE | re.DOTALL)
                    for matchNum, match in enumerate(matches):
                        for groupNum in range(0, len(match.groups())):
                            colName = match.group(1)
                            if colName and len(colName)>0:
                                colToReplace = self.__isColumnExists (colName=match.group(1), tarToSrc=tarToSrc)
                                if colToReplace:
                                    colName = colName.replace("{","").replace("}","")
                                    newExcecFunction.replace(colName,colToReplace)
                    execOnRowsDic[i] = newExcecFunction

            columnStr = ",".join(sourceColumnStr)

            sourceSql = qp.replaceSQLColumns(sqlStr=self.connSql, columnStr=columnStr)


        """ EXECUTING SOURCE QUERY """
        self.exeSQL(sql=sourceSql , commit=False)

        if len(targetColumnStr)==0:
            targetColumnStr = [col[0] for col in self.cursor.description]
        rows = None

        try:
            if batchRows and batchRows>0:
                while True:
                    rows = self.cursor.fetchmany( batchRows )
                    if not rows or len(rows) < 1:
                        break

                    rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic, execDict=execOnRowsDic)
                    tar.load (rows=rows, targetColumn = targetColumnStr)
            else:
                rows = self.cursor.fetchall()
                if fnOnRowsDic and len(fnOnRowsDic)>0:
                    rows = self.dataTransform(data=rows, functionDict=fnOnRowsDic)
                tar.load(rows, targetColumn = targetColumnStr)
        except Exception as e:
            p("TYPE:%s, OBJECT:%s ERROR FATCHING DATA" % (self.conn, str(self.connObj)), "e")
            p(str(e), "e")

    def load(self, rows, targetColumn):
        totalRows = len(rows) if rows else 0
        if totalRows == 0:
            p("THERE ARE NO ROWS")
            return
        tblFullName = "%s.%s" %(self.defaultSchema, self.connObj) if self.defaultSchema else self.connObj
        execQuery = "INSERT INTO %s" % (tblFullName)
        pre, pos = self.columnFrame[0], self.columnFrame[1]
        colList = []
        colInsert = []
        for col in targetColumn:
            colName = '%s%s%s' % (pre, col, pos)
            colList.append(colName)
            colInsert.append('?')
        execQuery += "(%s) " % (",".join(colList))
        execQuery += "VALUES (%s)" % (",".join(colInsert))

        try:
            self.cursor.executemany(execQuery, rows)
            self.connDB.commit()
            p('LOAD %s into target: %s >>>>>> ' % (str(totalRows), self.connObj), "ii")

        except Exception as e:
            p("TYPE:%s, OBJCT:%s ERROR in cursor.executemany !!!!" % (self.conn, str(self.connObj)), "e")
            p("ERROR QUERY:%s " % str(execQuery), "e")
            sampleRes = ['Null' if not r else "'%s'" % r for r in rows[0]]
            p("SAMPLE:%s " % str(", ".join(sampleRes)), "e")
            p(e, "e")
            if config.LOOP_ON_ERROR:
                iCnt = 0
                tCnt = len(rows)
                errDict = {}
                totalErrorToLooap = int(tCnt * 0.1)
                totalErrorsFound = 0
                p("ERROR, LOADING ROW BY ROW UT TO %s ERRORS OUT OF %s ROWS " % (str(totalErrorToLooap), str(tCnt)),
                  "e")
                for r in rows:
                    try:
                        iCnt += 1
                        r = [r]
                        self.cursor.executemany(execQuery, r)
                        self.conn.commit()
                    except Exception as e:
                        totalErrorsFound += 1
                        if totalErrorsFound > totalErrorToLooap:
                            break
                        errMsg = str(e).lower()

                        if errMsg not in errDict:
                            errDict[errMsg] = 0
                            ret = ""
                            for col in r[0]:
                                if col is None:
                                    ret += "Null, "
                                else:
                                    ret += "'%s'," % (col)
                            p(execQuery, "e")
                            p(ret, "e")
                            p(e, "e")
                        else:
                            errDict[errMsg] += 1

                p("ERROR ROW BY ROW: TOTAL ERROS:%s OUT OF %s, QUITING  " % (
                    str(totalErrorToLooap), str(tCnt)), "e")
                for err in errDict:
                    p("TOTAL ERRORS: %s, MSG: %s: " % (str(err), str(errDict[err])), "e")

    def execMethod(self):
        pass

    """ PUBLIC METHOD FOR DB MANIPULATION  """

    def exeSQL(self, sql, commit=True):
        s = ''
        if not (isinstance(sql, (list, tuple))):
            sql = [sql]
        try:
            for s in sql:
                self.cursor.execute(s)  # if 'ceodbc' in odbc.__name__.lower() else self.conn.execute(s)
            if commit:
                self.connDB.commit()  # if 'ceodbc' in odbc.__name__.lower() else self.cursor.commit()
            return True
        except Exception as e:
            p("baseConnDb->exeSQL:  ERROR: ", "e")
            p(e, "e")
            p("baseConnDb->exeSQL: ERROR SQL:\n %s " % (str(s)), "e")
            return False

    """ baseConnDB Method - Truncate table """
    def truncate(self, tableName=None, tableSchema=None):
        tableSchema, tableName  = self.setTableAndSchema (tableName=tableName, tableSchema=tableSchema, wrapTable=True)
        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.TRUNCATE, tableName=tableName, tableSchema=tableSchema)
        self.exeSQL(sql=sql)
        p("TYPE:%s, TRUNCATE TABLE:%s" % (self.conn, self.connObj),"ii")

    """ baseConnDB Method - Delete rows from table by using filter paramters"""
    def delete (self, sqlFilter, tableName=None, tableSchema=None):
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=True)
        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.DELETE, tableName=tableName, tableSchema=tableSchema, sqlFilter=sqlFilter)
        self.exeSQL(sql=sql)
        p("TYPE:%s, DELETE FROM TABLE:%s, WHERE:%s" % (self.conn, self.connObj, self.connFilter), "ii")


    """ Return TABLE STRUCTURE : {ColumnName:{Type:ColumnType, ALIACE: ColumnName} .... } """
    def getDBStructure(self, tableSchema, tableName):
        ret = OrderedDict()

        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.STRUCTURE, tableName=tableName, tableSchema=tableSchema)
        self.exeSQL(sql, commit=False)
        rows = self.cursor.fetchall()

        for col in rows:
            colName = decodePython2Or3(col[0], un=True)
            colType = decodePython2Or3(col[1], un=True)
            val = {eJson.jStrucure.TYPE: colType, eJson.jStrucure.ALIACE: None}
            ret[colName] = val
        return ret

    """ Return complex or simple QUERY STRUCURE:  {ColumnName:{Type:ColumnType, ALIACE: ColumnName} .... } """
    def getQueryStructure(self, sqlQuery=None):

        sqlQuery = sqlQuery if sqlQuery else self.connSql
        ret = OrderedDict()
        foundColumn = []
        pre = self.columnFrame[0]
        pos = self.columnFrame[1]
        noColumnsTables = []
        noMappingColumnsL = OrderedDict()

        if not sqlQuery or len(sqlQuery) < 1:
            return ret

        ### Return dictionary : {Table Name:[{SOURCE:ColumnName, ALIASE: column aliase}, ....]}
        ### And empty table -> all column that not related to any table '':[{SOURCE:columnName, ALIASE: .... } ...]
        queryTableAndColunDic = qp.extract_tableAndColumns(sql=sqlQuery, pre=pre, pos=pos)

        ## load all tables without any mapped column
        for tbl in queryTableAndColunDic:
            if not queryTableAndColunDic[tbl] or len(queryTableAndColunDic[tbl]) == 0:
                noColumnsTables.append(tbl)

        ## remove those table from main dictionary
        for tbl in noColumnsTables:
            del queryTableAndColunDic[tbl]

        ## find all column without any mapping table
        if '' in queryTableAndColunDic:
            for colD in queryTableAndColunDic['']:
                colName = colD[eJson.jSttValues.SOURCE]
                colAlias = colD[eJson.jSttValues.ALIACE]
                noMappingColumnsL[colName.replace(pre, "").replace(pos, "").lower()] = (colName, colAlias)
            del queryTableAndColunDic['']

        # update allTableStrucure dictionary : {tblName:{col name : ([original col name] , [tbl name] , [col structure])}}
        for tbl in queryTableAndColunDic:
            colDict = OrderedDict()
            tableNameList = tbl.split(".")
            tableName = tableNameList[0] if len(tableNameList) == 1 else tableNameList[1]
            tableSchema = tableNameList[0] if len(tableNameList) == 2 else self.defaultSchema
            for colD in queryTableAndColunDic[tbl]:
                colName = colD[eJson.jSttValues.SOURCE]
                colAlias = colD[eJson.jSttValues.ALIACE]
                colDict[colName.replace(pre, "").replace(pos, "").lower()] = (colName, colAlias,)

            tableStrucure = self.getDBStructure(tableSchema=tableSchema, tableName=tableName)

            tableColL = {x.replace(pre, "").replace(pos, "").lower(): x for x in tableStrucure}

            if '*' in colDict:
                ret.update(tableStrucure)

            for col in colDict:
                if col in tableColL:
                    ret[tableColL[col]] = tableStrucure[tableColL[col]]
                    ret[tableColL[col]][eJson.jStrucure.ALIACE] = colDict[col][1]
                else:
                    p("COLUMN %s NOT FOUND IN TABLE %s " % (tableColL[col], tbl), "ii")

            ## Search for Column that ther is no table mapping
            for col in noMappingColumnsL:
                if col in tableColL:
                    ret[tableColL[col]] = tableStrucure[tableColL[col]]
                    ret[tableColL[col]][eJson.jStrucure.ALIACE] = noMappingColumnsL[col][1]
                    foundColumn.append(col)

            ## Delete Column that has data type
            for col in foundColumn:
                if col in noMappingColumnsL:
                    del noMappingColumnsL[col]
            foundColumn = list([])

        # loop on all tables with no column and try to find match
        if noMappingColumnsL and len(noMappingColumnsL) > 0:
            for tbl in noColumnsTables:
                tableNameList = tbl.split(".")
                tableName = tableNameList[0] if len(tableNameList) == 1 else tableNameList[1]
                tableSchema = tableNameList[0] if len(tableNameList) == 2 else self.defaultSchema
                tableStrucure = self.getDBStructure(tableSchema=tableSchema, tableName=tableName)

                tableColL = {x.replace(pre, "").replace(pos, "").lower(): x for x in tableStrucure}

                if '*' in noMappingColumnsL:
                    ret.update(tableStrucure)
                    foundColumn.append('*')

                for col in noMappingColumnsL:
                    if col in tableColL:
                        ret[tableColL[col]] = tableStrucure[tableColL[col]]
                        ret[tableColL[col]][eJson.jStrucure.ALIACE] = noMappingColumnsL[col][1]
                        foundColumn.append(col)

                for col in foundColumn:
                    if col in noMappingColumnsL:
                        del noMappingColumnsL[col]
                foundColumn = list([])

            ## Add remaining column in not defined column types
            for col in noMappingColumnsL:
                colName = noMappingColumnsL[col][0]
                colAlias = noMappingColumnsL[col][1]
                ret[colName] = {eJson.jStrucure.TYPE: self.defDataType, eJson.jStrucure.ALIACE: colAlias}

        return ret

    """ INTERNAL USED for create method
        Create new Table is table is exsist
        If config.TRACK_HISTORY will save old table as tablename_currentDate   """
    def cloneObject(self, newStructure, tableSchema, tableName):
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=False)
        schemaEqual = True
        newStructureL = OrderedDict()
        pre,pos = self.columnFrame[0], self.columnFrame[1]

        for col in newStructure:
            colAlias = newStructure[col][eJson.jStrucure.ALIACE] if eJson.jStrucure.ALIACE in newStructure[col] else None
            colType  = newStructure[col][eJson.jStrucure.TYPE] if eJson.jStrucure.TYPE in newStructure[col] else self.defDatatType
            if colAlias:
                newStructureL[colAlias.replace(pre,"").replace(pos,"").lower()] = (colAlias,colType)
            else:
                newStructureL[col.replace(pre,"").replace(pos,"").lower()] = (col, colType)

        existStructure = self.getStructure (tableName=tableName, tableSchema=tableSchema,sqlQuery=None)


        if not existStructure or len(existStructure) == 0:
            p("baseConnDb->cloneObject: TABLE %s NOT EXISTS " %(tableName), "ii")
            return True

        existStructureL = {x.replace(pre, "").replace(pos, "").lower(): x for x in existStructure}

        for col in existStructureL:
            if col in newStructureL:
                if existStructure[ existStructureL[col] ][eJson.jStrucure.TYPE].lower() != newStructureL[ col ][1].lower():
                    schemaEqual = False
                    p("TYPE FOR COLUMN %s CHANGED, OLD: %s, NEW: %s" % (col, existStructure[col][eJson.jStrucure.TYPE], newStructureL[ col.lower() ][1]), "ii")
            else:
                schemaEqual = False
                p("TABLE CHANGED REMOVE COLUMN: %s " % (col), "ii")

        for col in newStructureL:
            if col not in existStructureL:
                schemaEqual = False
                p("TABLE CHANGED ADD COLUMN: %s " % (newStructureL[col][0]), "ii")

        if schemaEqual:
            p("baseConnDb->cloneObject: TABLE %s DID NOT CHANGED  >>>>>" % (tableName), "ii")
            return False
        else:
            if config.TRACK_HISTORY:
                p("baseConnDb->cloneObject: Table History is ON ...", "ii")
                newHistoryTable = "%s_%s" % (tableName, str(time.strftime('%y%m%d')))
                if (self.isExists(tableSchema=tableSchema, tableName=tableName)):
                    num = 0
                    while (self.isExists(tableSchema=tableSchema, tableName=newHistoryTable)):
                        num += 1
                        newHistoryTable = "%s_%s_%s" % (tableName, str(time.strftime('%y%m%d')), str(num))
                if newHistoryTable:
                    p("baseConnDb->cloneObject: Table History is ON and changed, table %s exists ... will rename to %s" % (str(tableName), str(newHistoryTable)), "ii")
                    sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.RENAME, tableSchema=tableSchema,tableName=tableName, tableNewName=newHistoryTable)

                    # sql = eval (self.objType+"_renameTable ("+self.objName+","+oldName+")")
                    p("baseConnDb->cloneObject: RENAME TABLE SQL:%s" % (str(sql)), "ii")
                    self.exeSQL(sql=sql, commit=True)
            else:
                if existStructure and len(existStructure)>0:
                    p("baseConnDb->cloneObject: TABLE HISTORY IS OFF AND TABLE EXISTS, CREATE TABLE %s IN NEW STRUCTURE... "%(str(tableName)), "ii")
                    sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.DROP,  tableName=tableName, tableSchema=tableSchema)

                    self.exeSQL(sql=sql, commit=True)
        return True

    """ Return tableSchema, tableName From table name and schema 
        WrapTable=True will return with DB wrapping for example Sql server colum yoyo will be [yoyo] """
    def setTableAndSchema (self, tableName, tableSchema=None, wrapTable=False):
        tableSchema = tableSchema if tableSchema else self.defaultSchema
        tableName   = tableName if tableName else self.connObj

        tableName = tableName.split (".")
        if len(tableName) == 1:
            tableName   = tableName[0]
        else:
            tableSchema = tableName[0]
            tableName   = tableName[1]

        tableSchema = self.wrapColName (tableSchema, remove=not wrapTable)
        tableName   = self.wrapColName (tableName, remove=not wrapTable)
        return tableSchema, tableName

    """ Wrap Column with DB brackets. Sample SqlServer column: productID --> [productId] """
    def wrapColName (self, col, remove=False):
        if self.columnFrame and col and len (col)>0:
            srcPre, srcPost = self.columnFrame
            coList = col.split(".")
            ret = ""
            for col in coList:
                col = col.replace(srcPre,"").replace(srcPost,"")
                if not remove:
                    col= "%s%s%s" %(srcPre,col,srcPost)
                ret+=col+"."
            return ret[:-1]
        return col

    """ Cnvert all paramters in config.QUERY_PARAMS into variable and add it into SQL QUERY """
    def setQueryWithParams(self, query):
        qRet = u""
        if query and len(query) > 0:
            if isinstance(query, (list, tuple)):
                for q in query:
                    for param in config.QUERY_PARAMS:
                        q = self.__replaceStr(sString=q, findStr=param, repStr=config.QUERY_PARAMS[param], ignoreCase=True,addQuotes="'")
                    qRet += q + u" "
            else:
                for param in config.QUERY_PARAMS:
                    if param in query:
                        query = self.__replaceStr(sString=query, findStr=param, repStr=config.QUERY_PARAMS[param],ignoreCase=True, addQuotes="'")
                qRet += query
            if len(config.QUERY_PARAMS)>0:
                p("baseConnDb->setQueryWithParams: replace params: %s " % (str(config.QUERY_PARAMS)), "ii")
        else:
            qRet = query
        return qRet

    """ INTERNAL METHOD  """

    """ Internal Method for replace paramters in setQueryWithParams Metod """
    def __replaceStr(self, sString, findStr, repStr, ignoreCase=True, addQuotes=None):
        if addQuotes and isinstance(repStr, str):
            repStr = "%s%s%s" % (addQuotes, repStr, addQuotes)

        if ignoreCase:
            pattern = re.compile(re.escape(findStr), re.IGNORECASE)
            res = pattern.sub(repStr, sString)
        else:
            res = sString.replace(findStr, repStr)
        return res

    def __isColumnExists (self, colName, tarToSrc):
        for ind, col in enumerate (tarToSrc):
            if col.lower() == colName.lower():
                return ind
            elif eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE].lower() == col.lower():
                return ind

        p("COLUMN %s NOT FOUND IN MAPPING" %(colName))
        return None


    def minValues (self, colToFilter=None, resolution=None, periods=None, startDate=None):
        raise NotImplementedError("minValues need to be implemented")

    def merge (self, mergeTable, mergeKeys=None, sourceTable=None):
        srcSchema, srcName = self.setTableAndSchema(tableName=sourceTable, tableSchema=None, wrapTable=True)
        mrgSchema, mrgName = self.setTableAndSchema(tableName=mergeTable, tableSchema=None, wrapTable=True)

        srcStructure    = self.getDBStructure(tableSchema=srcSchema, tableName=srcName)
        mrgStructure    = self.getDBStructure(tableSchema=mrgSchema, tableName=mrgName)

        mrgStructureL   = {x.lower():x for x in mrgStructure}
        srcStructureL   = {x.lower():x for x in srcStructure}
        mergeKeysL      = [x.lower() for x in mergeKeys]

        ### MERGE IDENTICAL COLUMN ONLY
        updateColumns       = []
        keyColumns          = []
        allColumns          = []
        notExistsInMergeCol = []
        notExistsInSourceCol= []

        for col in srcStructure:
            if col.lower() in mrgStructureL:
                col = self.wrapColName(col=col, remove=False)
                allColumns.append (col)
                if col.lower() in mergeKeysL:
                    keyColumns.append(col)
                else:
                    updateColumns.append(col)
            else:
                notExistsInMergeCol.append (col)

        for col in mrgStructure:
            if col.lower() not in srcStructureL:
                notExistsInSourceCol.append (col)

        if len (notExistsInMergeCol)>0:
            p ("SOURCE COLUMNS %s NOT EXISTS IN MERGE TABLE %s" %(str(notExistsInMergeCol), sourceTable),"ii")

        if len (notExistsInSourceCol)>0:
            p ("MERGE COLUMNS %s NOT EXISTS IN SOURCE TABLE %s" %(str(notExistsInSourceCol), mergeTable),"ii")

        if len (keyColumns) == 0:
            keyColumns = updateColumns

        dstTable = '%s.%s' %(mrgSchema,mrgName) if mrgSchema else mrgName
        srcTable = '%s.%s' %(srcSchema,srcName)

        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.MERGE, dstTable=dstTable, srcTable=srcTable, mergeKeys=keyColumns, colList=updateColumns, colFullList=allColumns)
        self.exeSQL(sql=sql)
        p("TYPE:%s, MERGE %s WITH %s, \n\t\tMERGE KEYS:%s" %(self.conn, srcTable, dstTable, str(keyColumns)), "ii")

    def cntRows (self):
        raise NotImplementedError("count rows need to be implemented")