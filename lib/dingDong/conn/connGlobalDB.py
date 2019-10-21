# -*- coding: utf-8 -*-
# (c) 2017-2019, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of dingDong
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any late r version.
#
# dingDong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDong.  If not, see <http://www.gnu.org/licenses/>.

import os
import io
import sys
import time
import traceback
from collections import OrderedDict

from dingDong.conn.baseBatch            import baseBatch
from dingDong.conn.baseBatchFunction    import *
from dingDong.misc.enumsJson            import eConn, eJson, eSql
from dingDong.misc.misc                 import uniocdeStr
from dingDong.config                    import config
from dingDong.misc.logger               import p
import dingDong.conn.connGlobalDbQueryParser as qp
from dingDong.conn.connGlobalDbSqlQueries import setSqlQuery
from dingDong.executers.executeSql import execQuery

try:
    import cx_Oracle  # version : 6.1
except ImportError:
    p("cx_Oracle is not installed", "ii")

DEFAULTS = {
            eConn.NONO: {   eJson.jValues.DEFAULT_TYPE:'varchar(100)',eJson.jValues.SCHEMA:'dbo',
                            eJson.jValues.EMPTY:'Null',eJson.jValues.COLFRAME:("[","]"),
                            eJson.jValues.SP:{'match':None, 'replace':None}, eJson.jValues.UPDATABLE:False},

            eConn.ORACLE: {eJson.jValues.DEFAULT_TYPE: 'varchar(100)', eJson.jValues.SCHEMA: 'dbo',
                           eJson.jValues.EMPTY: 'Null', eJson.jValues.COLFRAME: ('"', '"'),
                           eJson.jValues.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}, eJson.jValues.UPDATABLE:True},

            eConn.SQLSERVER: {eJson.jValues.DEFAULT_TYPE: 'varchar(100)', eJson.jValues.SCHEMA: 'dbo',
                              eJson.jValues.EMPTY: 'Null', eJson.jValues.COLFRAME: ("[", "]"),
                              eJson.jValues.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}, eJson.jValues.UPDATABLE:True  },

            eConn.POSTGESQL: {eJson.jValues.DEFAULT_TYPE: 'varchar(100)', eJson.jValues.SCHEMA: 'public',
                              eJson.jValues.EMPTY: 'Null', eJson.jValues.COLFRAME: ('"', '"'),
                              eJson.jValues.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}, eJson.jValues.UPDATABLE:True  },

            eConn.LITE: {   eJson.jValues.DEFAULT_TYPE:'varchar(100)',eJson.jValues.SCHEMA:None,
                            eJson.jValues.EMPTY:'Null', eJson.jValues.UPDATABLE:False}
           }

DATA_TYPES = {
    eConn.ORACLE:   { eConn.dataType.DB_DATE:['date','datetime'],
                      eConn.dataType.DB_VARCHAR:['varchar','varchar2'],
                      eConn.dataType.DB_DECIMAL:['number','numeric','dec','decimal']
                    },
    eConn.SQLSERVER:{
                        eConn.dataType.DB_DATE:['smalldatetime','datetime'],
                        eConn.dataType.DB_DECIMAL:['decimal']
                    },

    eConn.POSTGESQL:{
                        eConn.dataType.DB_DATE:['smalldatetime','datetime'],
                        eConn.dataType.DB_DECIMAL:['decimal']
                    },

    eConn.ACCESS: { eConn.dataType.DB_VARCHAR:['varchar', 'longchar', 'bit', 'ntext'],
                    eConn.dataType.DB_INT:['integer', 'counter'],
                    eConn.dataType.DB_FLOAT:['double'],
                    eConn.dataType.DB_DECIMAL:['decimal']
                    }
}

class baseGlobalDb (baseBatch):

    def __init__ (self, connPropDict=None, conn=None, connUrl=None, connExtraUrl=None,
                  connName=None,connObj=None,  connFilter=None, connIsTar=None,
                  connIsSrc=None, connIsSql=None,
                  defaults=None, dataType=None):

        defaults = defaults if defaults else DEFAULTS
        dataType = dataType if dataType else DATA_TYPES
        baseBatch.__init__(self, conn=conn, connName=connName, connPropDict=connPropDict, defaults=defaults, dataType=dataType)
        self.usingSchema= True

        """ BASIC PROPERTIES FROM BASECONN """
        self.conn               = self.conn
        self.connName           = self.connName
        self.defDataType        = self.defDataType
        self.update             = self.update
        self.creeateFromObjName = self.creeateFromObjName

        """ DB PROPERTIES """
        self.connUrl = self.setProperties(propKey=eJson.jValues.URL, propVal=connUrl)
        self.connExtraUrl = self.setProperties(propKey=eJson.jValues.URLPARAM, propVal=connExtraUrl)

        self.connObj    = self.setProperties (propKey=eJson.jValues.OBJ, propVal=connObj)
        self.connFilter = self.setProperties (propKey=eJson.jValues.FILTER, propVal=connFilter)
        self.connIsSrc  = self.setProperties (propKey=eJson.jValues.IS_SOURCE, propVal=connIsSrc)
        self.connIsTar  = self.setProperties (propKey=eJson.jValues.IS_TARGET, propVal=connIsTar)
        self.connIsSql  = self.setProperties (propKey=eJson.jValues.IS_SQL, propVal=connIsSql)
        self.sqlFolder  = self.setProperties (propKey=eJson.jValues.FOLDER, propVal=None)
        self.sqlFullFile= self.setProperties(propKey=eJson.jValues.URL_FILE, propVal=None)

        self.defaultSchema  = self.DEFAULTS[eJson.jValues.SCHEMA]
        self.defaulNull     = self.DEFAULTS[eJson.jValues.EMPTY]
        self.defaultSP      = self.DEFAULTS[eJson.jValues.SP]
        self.columnFrame    = self.DEFAULTS[eJson.jValues.COLFRAME]

        self.cursor         = None
        self.connDB         = None
        self.connSql        = None

        self.isExtractSqlIsOnlySTR  = False
        self.parrallelProcessing    = False

        if not self.connUrl:
            self.connUrl = connPropDict

        if self.connIsSql:
            self.connSql    = self.setQueryWithParams(self.connObj)
            self.connObj    = self.connSql

        elif self.connObj and len(self.connObj)>0 \
                and ('.sql' not in self.connObj and (not self.sqlFullFile or (self.sqlFullFile and self.sqlFullFile not in self.connObj))):

            self.connSql = "SELECT * FROM %s" %self.connObj

            if self.connIsSrc or self.connIsTar:
                self.connObj        = self.wrapColName(col=self.connObj, remove=True).split(".")
                self.defaultSchema  = self.connObj[0] if len(self.connObj) > 1 else self.defaultSchema
                self.connObj        = self.connObj[1] if len(self.connObj) > 1 else self.connObj[0]

            if self.connFilter and len(self.connFilter) > 1:
                self.connFilter = re.sub(r'WHERE', '', self.connFilter, flags=re.IGNORECASE)
                self.connSql = '%s WHERE %s' %(self.connSql, self.setQueryWithParams(self.connFilter))

        self.__mapObjectToFile()
        objName = "QUERY " if self.connIsSql else "TABLE: %s" %self.connObj

        self.connect()

        p("CONNECTED, DB TYPE: %s, %s" % (self.conn, objName, ), "ii")

    def connect(self):
        try:
            if eConn.MYSQL == self.conn:
                import pymysql
                self.connDB = pymysql.connect(self.connUrl["host"], self.connUrl["user"], self.connUrl["passwd"],
                                              self.connUrl["db"])
                self.cursor = self.connDB.cursor()

            elif eConn.POSTGESQL == self.conn:
                import psycopg2
                self.connDB = psycopg2.connect(self.connUrl)
                self.cursor = self.connDB.cursor()

            elif eConn.VERTICA == self.conn:
                import vertica_python
                self.connDB = vertica_python.connect(self.connUrl)
                self.cursor = self.connDB.cursor()

            elif eConn.ORACLE == self.conn:
                self.isExtractSqlIsOnlySTR = True
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
            err = "Error connecting into DB: %s, ERROR: %s\n " % (self.conn, str(e))
            err += "USING URL: %s\n" %(self.connUrl)
            #err+= traceback.format_exc()
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
            p("ERROR: file name:"+str(fname)+" line: "+str(exc_tb.tb_lineno)+" massage: "+str(exc_obj), "e")

    def test(self):
        baseBatch.test(self)

    def isExists(self, tableName, tableSchema):
        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.ISEXISTS, tableName=tableName, tableSchema=tableSchema)
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        if row and row[0]:
            return True
        p("SCHEMA:%s, TABLE:%s NOT EXISTS" % (tableSchema, tableName), "ii")
        return False

    def create(self, stt=None, objName=None, addIndex=None):
        tableSchema, tableName = self.setTableAndSchema(tableName=objName, tableSchema=None, wrapTable=False)
        tableFullName = '%s.%s'%(tableSchema, tableName) if tableSchema else tableName

        if not stt or len(stt) == 0:
            p("TABLE %s NOT MAPPED CORRECLTY " %(self.connObj), "e")
            return

        isNew,isChanged, newHistoryTable = self.cloneObject(newStructure=stt, tableName=tableName, tableSchema=tableSchema)

        if isNew or  (isChanged and (self.update in [eJson.jUpdate.UPDATE, eJson.jUpdate.DROP])):
            sql = "CREATE TABLE %s \n (" %(tableFullName)
            for col in stt:
                if eJson.jSttValues.ALIACE in stt[col] and stt[col][eJson.jSttValues.ALIACE] and len (stt[col][eJson.jSttValues.ALIACE])>0:
                    colName = self.wrapColName (col=stt[col][eJson.jSttValues.ALIACE], remove=True)
                else:
                    colName =  self.wrapColName (col=col, remove=True)
                colType =  stt[col][eJson.jSttValues.TYPE]
                sql += '\t%s\t%s,\n' %(colName, colType)

            sql = sql[:-2]+')'
            p("CREATE TABLE: \n" + sql)
            self.exeSQL(sql=sql, commit=True)
            if self.versionManager: self.versionManager(sql)

        # Check for index
        if addIndex and self.update != eJson.jUpdate.NO_UPDATE:
            self.addIndexToTable(tableName, addIndex)

        if config.DING_ADD_OBJECT_DATA:
            if newHistoryTable and len (newHistoryTable)>0:
                columns = []
                pre, pos = self.columnFrame[0], self.columnFrame[1]
                oldStructure = self.getStructure(tableName=newHistoryTable, tableSchema=tableSchema, sqlQuery=None)
                newStructure = self.getStructure(tableName=tableName, tableSchema=tableSchema, sqlQuery=None)

                oldStructureL = {x.replace(pre, "").replace(pos, "").lower(): x for x in oldStructure}
                newStructureL = {x.replace(pre, "").replace(pos, "").lower(): x for x in newStructure}

                for col in oldStructureL:
                    if col in newStructureL:
                        columns.append (self.wrapColName(col, remove=False))

                if len (columns)>0:
                    sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.TABLE_COPY_BY_COLUMN, tableName=tableName,tableSchema=tableSchema, srcTableName=newHistoryTable, columns=columns )
                    self.exeSQL(sql=sql)
                    p("ADD ROWS TO %s FROM %s, UPDATED COLUMNS:\n%s" %(tableName, newHistoryTable, ",".join(columns)), "w")

    """ INTERNAL USED: for create method: Compare existing stucture to new one, if 
            object exists call compareExistToNew - compare 2 object and update   """
    def cloneObject(self, newStructure, tableName, tableSchema=None):
        newStructureL   = OrderedDict()
        isNew           = True
        isChanged       = False

        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=False)
        pre, pos = self.columnFrame[0], self.columnFrame[1]

        for col in newStructure:
            colAlias = newStructure[col][eJson.jSttValues.ALIACE] if eJson.jSttValues.ALIACE in newStructure[
                col] else None
            colType = newStructure[col][eJson.jSttValues.TYPE] if eJson.jSttValues.TYPE in newStructure[
                col] else self.defDataType
            if colAlias:
                newStructureL[colAlias.replace(pre, "").replace(pos, "").lower()] = (colAlias, colType)
            else:
                newStructureL[col.replace(pre, "").replace(pos, "").lower()] = (col, colType)

        existStructure = self.getStructure(tableName=tableName, tableSchema=tableSchema, sqlQuery=None)

        if not existStructure or len(existStructure) == 0:
            p("TABLE %s NOT EXISTS " % (tableName), "ii")
            return isNew, isChanged, None

        isNew = False
        isChanged, newHistoryTable = self.compareExistToNew(existStructure, newStructureL, tableName, tableSchema)
        return isNew, isChanged, newHistoryTable

    """ Strucutre Dictinary: {Column Name: {ColumnType:XXXXX, ColumnAliace: NewColumns } .... } 
        Help methods: getQueryStructure (baseConnDB), getDBStructure (baseConnDB)"""
    def getStructure(self, tableName=None, tableSchema=None, sqlQuery=None):
        sqlQuery = sqlQuery if sqlQuery else self.connSql
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema)
        retStructure = None
        pre, pos = self.columnFrame[0], self.columnFrame[1]

        # If there is query and there is internal maaping in query - will add this mapping to mappingColum dictionary
        if self.connIsSql:
            retStructure = self.getQueryStructure(sqlQuery=sqlQuery)

        elif tableName and len(tableName) > 0:
            retStructure = self.getDBStructure(tableName=tableName, tableSchema=tableSchema)

        finallStructure = OrderedDict()
        for col in retStructure:
            # col = col.replace (pre,"").replace(pos,"")
            finallStructure[col] = retStructure[col]

            if eJson.jSttValues.ALIACE in finallStructure[col] and finallStructure[col][
                eJson.jSttValues.ALIACE] is not None:
                finallStructure[col][eJson.jSttValues.ALIACE] = finallStructure[col][eJson.jSttValues.ALIACE].replace(
                    pre, "").replace(pos, "")

        return finallStructure

    """ INTERNAL USED: TABLE STRUCTURE : {ColumnName:{Type:ColumnType, ALIACE: ColumnName} .... } """
    def getDBStructure(self, tableName, tableSchema):
        ret = OrderedDict()

        if not self.isExists(tableName=tableName, tableSchema=tableSchema):
            p ("%s: TABLE %s, SCHEMA %s NOT EXISTS.. " %(self.conn, tableName, tableSchema), "ii")
            return ret

        tableSchema = self.wrapColName(col=tableSchema, remove=True)
        tableName = self.wrapColName(col=tableName, remove=True)
        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.STRUCTURE, tableName=tableName, tableSchema=tableSchema)
        self.exeSQL(sql, commit=False)

        rows = self.cursor.fetchall()
        if not rows or (rows and len(rows) < 1):
            p("ERROR CONN: %s RECEIVE DB STRACTURE: TABLE: %s, SCHEMA: %s" % (self.conn, tableName, tableSchema), "e")

        for col in rows:
            colName = uniocdeStr(col[0])
            colType = uniocdeStr(col[1])
            val = {eJson.jSttValues.TYPE: colType, eJson.jSttValues.ALIACE: None}
            ret[colName] = val

        return ret

    """ INTERNAL USED: Complex or simple QUERY STRUCURE:  {ColumnName:{Type:ColumnType, ALIACE: ColumnName} .... } """
    def getQueryStructure(self, sqlQuery=None):

        sqlQuery = sqlQuery if sqlQuery else self.connSql
        ret = OrderedDict()
        pre = self.columnFrame[0]
        pos = self.columnFrame[1]

        notFoundColumns = {}
        allFoundColumns = []
        foundColumns = []

        if not sqlQuery or len(sqlQuery) < 1:
            return ret

        ### Return dictionary : {Table Name:[{SOURCE:ColumnName, ALIASE: column aliase}, ....]}
        ### And empty table -> all column that not related to any table '':[{SOURCE:columnName, ALIASE: .... } ...]
        queryTableAndColunDic = qp.extract_tableAndColumns(sql=sqlQuery)

        if qp.QUERY_COLUMNS_KEY in queryTableAndColunDic:
            allFoundColumns = queryTableAndColunDic[qp.QUERY_COLUMNS_KEY]
            del queryTableAndColunDic[qp.QUERY_COLUMNS_KEY]

        if qp.QUERY_NO_TABLE in queryTableAndColunDic:
            for colD in queryTableAndColunDic[qp.QUERY_NO_TABLE][qp.TABLE_COLUMN]:
                colTupleName = colD[0]
                colTargetName = colD[1]
                colName = colTupleName[-1]
                colFullName = ".".join(colTupleName)
                notFoundColumns[colTargetName] = (colName, colFullName,)

            del queryTableAndColunDic[qp.QUERY_NO_TABLE]

        if qp.QUERY_POST in queryTableAndColunDic:
            del queryTableAndColunDic[qp.QUERY_POST]

        if qp.QUERY_PRE in queryTableAndColunDic:
            del queryTableAndColunDic[qp.QUERY_PRE]

        # update allTableStrucure dictionary : {tblName:{col name : ([original col name] , [tbl name] , [col structure])}}
        for tbl in queryTableAndColunDic:
            colDict = OrderedDict()
            tableName = tbl
            tableSchema = queryTableAndColunDic[tbl][qp.TABLE_SCHEMA]
            tableColumns = queryTableAndColunDic[tbl][qp.TABLE_COLUMN]

            for colD in tableColumns:
                colTupleName = colD[0]
                colTargetName = colD[1]
                colName = colTupleName[-1]
                colFullName = ".".join(colTupleName)
                colDict[colTargetName] = (colName, colFullName)

            tableStrucure = self.getDBStructure(tableName=tableName, tableSchema=tableSchema)
            tableColL = {x.replace(pre, "").replace(pos, "").lower(): x for x in tableStrucure}

            for col in colDict:
                colTarName = col
                colSName = colDict[col][0]
                colLName = colDict[col][1]
                isFound = False
                if '*' in colSName:
                    isFound = True
                    ret.update(tableStrucure)
                else:
                    for colTbl in tableColL:
                        if colTbl.lower() in colSName.lower():
                            ret[colTarName] = {eJson.jSttValues.SOURCE: tableColL[colTbl],
                                               eJson.jSttValues.TYPE: tableStrucure[tableColL[colTbl]][
                                                   eJson.jSttValues.TYPE]}
                            isFound = True
                            break

                if not isFound:
                    p("COLUMN %s NOT FOUND IN TABLE %s USING DEFAULT %s" % (col, tbl, self.defDataType), "ii")
                    ret[colTarName] = {eJson.jSttValues.SOURCE: colLName, eJson.jSttValues.TYPE: self.defDataType}

            ## Search for Column that ther is no table mapping
            for col in notFoundColumns:
                colTarName = col
                colSName = notFoundColumns[col][0]
                colLName = notFoundColumns[col][1]
                for colTbl in tableColL:
                    if colTbl.lower() in colSName.lower():
                        ret[colTarName] = {eJson.jSttValues.SOURCE: colLName,
                                           eJson.jSttValues.TYPE: tableStrucure[tableColL[colTbl]][
                                               eJson.jSttValues.TYPE]}
                        foundColumns.append(colLName)
                        break

        for col in notFoundColumns:
            colTarName = col
            colSName = notFoundColumns[col][0]
            colLName = notFoundColumns[col][1]
            if colLName not in foundColumns:
                p("COLUMN %s NOT FOUND IN ANY TABLE, USING DEFAULT %s" % (colLName, self.defDataType), "ii")
                ret[colTarName] = {eJson.jSttValues.SOURCE: colLName, eJson.jSttValues.TYPE: self.defDataType}


        retOrder = OrderedDict()
        for col in allFoundColumns:
            if col[1] in ret:
                retOrder[col[1]] = ret[col[1]]
            else:
                p("!!!!ERROR: COLUMN %s NOT FOUND " % col[1], "w")
        for col in ret:
            if col not in retOrder:
                p("!!!!ERROR: COLUMN %s NOT FOUND " % col, "w")

        return ret

    """ INTERNAL USED: for clone method - update object is exists
            -1: create history table base on config.TRACK_HISTORY. new name: tablename_currentDate
            1 : update strucure (change data type, add column or remove column)
            2 : Cannot change object """
    def compareExistToNew(self, existStructure, newStructureL, tableName, tableSchema):
        isChanged       = False
        newHistoryTable = None
        updateDesc = 'UPDATE' if self.update == eJson.jUpdate.UPDATE else 'DROP CREATE' if self.update == eJson.jUpdate.DROP else 'WARNIN, NO UPDATE'
        pre, pos = self.columnFrame[0], self.columnFrame[1]
        existStructureL = {x.replace(pre, "").replace(pos, "").lower(): x for x in existStructure}

        for col in existStructureL:
            if col in newStructureL:
                ## update column type
                if existStructure[existStructureL[col]][eJson.jSttValues.TYPE].lower() != newStructureL[col][1].lower():
                    isChanged = True
                    if self.update == eJson.jUpdate.UPDATE:
                        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.COLUMN_UPDATE,
                                                   tableName=tableName, tableSchema=tableSchema, columnName=existStructureL[col],
                                                   columnType=newStructureL[col][1])
                        self.exeSQL(sql=sql)
                        if self.versionManager: self.versionManager(sql)
                    p("%s: CONN:%s, TABLE: %s, COLUMN %s, TYPE CHANGED, OLD: %s, NEW: %s" % (updateDesc, self.conn, tableName, col, existStructure[existStructureL[col]][eJson.jSttValues.TYPE],newStructureL[col][1]), "w")

            ## REMOVE COLUMN
            else:
                isChanged = True
                if self.update == eJson.jUpdate.UPDATE:
                    sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.COLUMN_DELETE, tableName=tableName,
                                               tableSchema=tableSchema, columnName=existStructureL[col])
                    self.exeSQL(sql=sql)
                    if self.versionManager: self.versionManager(sql)
                p("CONN:%s, TABLE: %s, REMOVING COLUMN: %s " % (self.conn, tableName, col), "w")

        for col in newStructureL:
            # ADD COLUMN
            if col not in existStructureL:
                isChanged = True
                if self.update == eJson.jUpdate.UPDATE:
                    sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.COLUMN_ADD,
                                               tableName=tableName, tableSchema=tableSchema, columnName=newStructureL[col][0],
                                               columnType=newStructureL[col][1])
                    self.exeSQL(sql=sql)
                    if self.versionManager: self.versionManager(sql)

                p("CONN:%s, TABLE: %s, ADD COLUMN: %s " % (self.conn, tableName, newStructureL[col][0]), "w")

        if not isChanged:
            p("TABLE %s DID NOT CHANGED  >>>>>" % (tableName), "ii")
            return isChanged, newHistoryTable
        else:
            if self.update > 1:
                p("TABLE STRUCTURE CHANGED, UPDATE IS NOT ALLOWED, NO CHANGE", "w")
                return isChanged, newHistoryTable
            else:
                if config.DING_TRACK_OBJECT_HISTORY:
                    p("TABLE HISTORY IS ON ...", "ii")
                    newHistoryTable = "%s_%s" % (tableName, str(time.strftime('%y%m%d')))
                    if (self.isExists(tableSchema=tableSchema, tableName=tableName)):
                        num = 0
                        while (self.isExists(tableSchema=tableSchema, tableName=newHistoryTable)):
                            num += 1
                            newHistoryTable = "%s_%s_%s" % (tableName, str(time.strftime('%y%m%d')), str(num))
                    if newHistoryTable:
                        p("TABLE HISTORY IS ON AND CHANGED, TABLE %s EXISTS ... RENAMED TO %s" % (
                        str(tableName), str(newHistoryTable)), "w")
                        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.RENAME, tableSchema=tableSchema,
                                                   tableName=tableName, tableNewName=newHistoryTable)

                        # sql = eval (self.objType+"_renameTable ("+self.objName+","+oldName+")")
                        p("RENAME TABLE SQL:%s" % (str(sql)), "w")
                        self.exeSQL(sql=sql, commit=True)
                else:
                    if existStructure and len(existStructure) > 0:
                        p("TABLE HISTORY IS OFF AND TABLE EXISTS, DROP -> CREATE TABLE %s IN NEW STRUCTURE... " % (
                            str(tableName)), "w")
                        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.DROP, tableName=tableName,
                                                   tableSchema=tableSchema)
                        self.exeSQL(sql=sql, commit=True)
        return isChanged, newHistoryTable

    """ INTERNAL USED: Add index """
    def addIndexToTable (self, tableName, addIndex, tableSchema=None):
        if addIndex and len(addIndex)>0:
            newIndexList    = []
            existIndexDict  = {}
            isClusterExists = False
            toCreate        = True

            ## update existing index
            addIndex = [addIndex] if isinstance(addIndex, (dict,OrderedDict)) else addIndex
            for ind in addIndex:
                if eJson.jValues.INDEX_COLUMS in ind and ind[eJson.jValues.INDEX_COLUMS]:
                    columns  = ind[eJson.jValues.INDEX_COLUMS] if isinstance(ind[eJson.jValues.INDEX_COLUMS], list) else [ind[eJson.jValues.INDEX_COLUMS]]
                    columns  = [x.lower() for x in columns]
                    isCluster= ind[eJson.jValues.INDEX_CLUSTER] if eJson.jValues.INDEX_CLUSTER in ind and ind[eJson.jValues.INDEX_CLUSTER] is not None else False
                    isUnique = ind[eJson.jValues.INDEX_UNIQUE] if eJson.jValues.INDEX_UNIQUE in ind and ind[eJson.jValues.INDEX_UNIQUE] is not None else False
                    newIndexList.append ({eJson.jValues.INDEX_COLUMS:columns, eJson.jValues.INDEX_CLUSTER:isCluster, eJson.jValues.INDEX_UNIQUE:isUnique})
                else:
                    p("NOT VALID INDEX %s , MUST HAVE COLUNs, IGNORING" %(str(ind)), "i")


            ## update newIndexDict from db.
            tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema)
            tableName = '%s.%s' %(tableSchema,tableName) if tableSchema else tableName
            # check if there is cluster index
            sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.INDEX_EXISTS, tableName=tableName)
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            if rows and len (rows)>0:
                for row in rows:
                    indexName = row[0]
                    if indexName not in existIndexDict:
                        existIndexDict[indexName] = {eJson.jValues.INDEX_COLUMS:[], eJson.jValues.INDEX_CLUSTER:False,eJson.jValues.INDEX_UNIQUE:False}

                    existIndexDict[indexName][eJson.jValues.INDEX_CLUSTER] = True if row[2] or str(row[2]) == '1' else False
                    if existIndexDict[indexName][eJson.jValues.INDEX_CLUSTER] == True:
                        isClusterExists = True
                    existIndexDict[indexName][eJson.jValues.INDEX_UNIQUE] = True if row[3] or str(row[3])  == '1' else False
                    existIndexDict[indexName][eJson.jValues.INDEX_COLUMS].append (str(row[1]).lower())

            ### Compare - Remove existing idential Indexes
            for eInd in existIndexDict:
                if existIndexDict[eInd] in newIndexList:
                    newIndexList.remove(existIndexDict[eInd])

            ### Compare index with existing indexes
            for ind in newIndexList:
                if ind[eJson.jValues.INDEX_CLUSTER] == True and isClusterExists:
                    p("CLUSTERED INDEX ALREADY EXISTS, IGNORE NEW INDEX:%s" %(str(ind)), "w")
                    toCreate = False
                else:
                    newColumn = ind[eJson.jValues.INDEX_COLUMS]

                    for eInd in existIndexDict:
                        eColumn = existIndexDict[eInd][eJson.jValues.INDEX_COLUMS]

                        if set(eColumn) == set(newColumn):
                            msg = """
                                    INDEX ON COLUMN %s EXISTS, OLD IS_CLUSTER:%s,
                                    OLD IS_UNIQUE:%s, NEW IS_CLUSTER: %s,
                                    NEW IS_UNIQUE: %s, IGNORING...""" %(str(eColumn),
                                                                         str(existIndexDict[eInd][eJson.jValues.INDEX_CLUSTER]),
                                                                         str(existIndexDict[eInd][eJson.jValues.INDEX_UNIQUE]),
                                                                         str(ind[eJson.jValues.INDEX_CLUSTER]),
                                                                         str(ind[eJson.jValues.INDEX_UNIQUE]))
                            p(msg , "i")
                            toCreate = False
                    if toCreate:
                        columnsCreate   = ind[eJson.jValues.INDEX_COLUMS]
                        isUnique        = ind[eJson.jValues.INDEX_UNIQUE]
                        isCluster       = ind[eJson.jValues.INDEX_CLUSTER]
                        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.INDEX, tableName=tableName, columns=columnsCreate, isCluster=isCluster, isUnique=isUnique)
                        self.exeSQL(sql=sql)
                        p("TYPE:%s, ADD INDEX: COLUNS:%s, CLUSTER:%s, UNIQUE:%s\n SQL: %s" % (self.conn, str(columnsCreate),str(isCluster),str(isUnique),str(sql)), "ii")

    def preLoading(self, tableName=None, tableSchema=None, sqlFilter=None):
        sqlFilter = sqlFilter if sqlFilter else self.connFilter
        if sqlFilter and len(sqlFilter) > 0:
            self.delete(sqlFilter=sqlFilter, tableName=tableName, tableSchema=tableSchema)
        else:
            self.truncate(tableName=tableName, tableSchema=tableSchema)

    """ INTERNAL USED: preLoading method """
    def truncate(self, tableName=None, tableSchema=None):
        tableSchema, tableName  = self.setTableAndSchema (tableName=tableName, tableSchema=tableSchema, wrapTable=True)
        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.TRUNCATE, tableName=tableName, tableSchema=tableSchema)
        self.exeSQL(sql=sql)
        p("TYPE:%s, TRUNCATE TABLE:%s" % (self.conn, self.connObj),"ii")

    """ INTERNAL USED: preLoading method """
    def delete (self, sqlFilter, tableName=None, tableSchema=None):
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=True)
        sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.DELETE, tableName=tableName, tableSchema=tableSchema, sqlFilter=sqlFilter)
        self.exeSQL(sql=sql)
        p("TYPE:%s, DELETE FROM TABLE:%s, WHERE:%s" % (self.conn, self.connObj, self.connFilter), "ii")

    def extract(self, tar, tarToSrc, batchRows=None, addAsTaret=True):
        batchRows = batchRows if batchRows else self.batchSize
        fnOnRowsDic     = {}
        execOnRowsDic   = {}
        pre,pos         = self.columnFrame[0],self.columnFrame[1]
        sourceColumnStr = []
        targetColumnStr = []
        sourceSql       = self.connSql

        ## There is Source And Target column mapping
        if tarToSrc and len (tarToSrc)>0:
            existingColumnsL            = OrderedDict()
            existingColumnsByTargetL    = OrderedDict()
            existingColumnsLFull        = OrderedDict()
            existingColumns = qp.extract_tableAndColumns(sql=sourceSql)

            preSql = existingColumns[qp.QUERY_PRE]
            postsql = existingColumns[qp.QUERY_POST]
            pre, pos = self.columnFrame[0], self.columnFrame[1]
            if self.connIsSql:
                allColumns  = existingColumns[qp.QUERY_COLUMNS_KEY]

                for col in allColumns:
                    existingColumnsL[col[0][-1].replace(pre,"").replace(pos,"").lower()] = ".".join(col[0])
                    existingColumnsLFull[".".join(col[0]).replace(pre, "").replace(pos, "").lower()] = ".".join(col[0])
                    existingColumnsByTargetL[ col[1].replace(pre,"").replace(pos,"").lower() ] = ".".join(col[0])

            else:
                allColumns = self.getStructure()
                for col in allColumns:
                    existingColumnsL[col.replace(pre,"").replace(pos,"").lower()] = col

            for i,col in  enumerate (tarToSrc):
                tarColumn = col.replace(pre, "").replace(pos, "")
                tarColumnName = '%s%s%s' % (pre, tarColumn, pos)
                if eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE]:
                    srcColumnName = tarToSrc[col][eJson.jSttValues.SOURCE].replace(pre, "").replace(pos, "").lower()
                    if srcColumnName in existingColumnsL:
                        srcColumnName = '%s As %s' % (existingColumnsL[srcColumnName], tarColumnName) if addAsTaret else existingColumnsL[srcColumnName]
                    elif srcColumnName in existingColumnsLFull:
                        srcColumnName = '%s As %s' % (existingColumnsLFull[srcColumnName], tarColumnName) if addAsTaret else existingColumnsLFull[srcColumnName]
                    elif srcColumnName in existingColumnsByTargetL:
                        srcColumnName = '%s As %s' % (existingColumnsByTargetL[srcColumnName], tarColumnName) if addAsTaret else existingColumnsByTargetL[srcColumnName]
                    else:
                        p("%s: %s, SOURCE COLUMN LISTED IN STT NOT EXISTS IN SOURCE TABLE, IGNORE COLUMN !!!!, OBJECT:\n%s" % (self.conn, tarToSrc[col][eJson.jSttValues.SOURCE], self.connObj), "e")
                        continue
                elif tarColumn.lower() in existingColumnsL:
                    srcColumnName = '%s As %s' %(existingColumnsL[ tarColumn.lower() ], tarColumnName) if addAsTaret else existingColumnsL[ tarColumn.lower() ]
                else:
                    srcColumnName =  "'' As %s" %(tarColumnName) if addAsTaret else ''

                sourceColumnStr.append (srcColumnName)
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
                                colToReplace = match.group(1).replace("{","").replace("}","")
                                colToReplace = self.__isColumnExists (colName=colToReplace, tarToSrc=tarToSrc)
                                if colToReplace:
                                    newExcecFunction = newExcecFunction.replace(colName,"{"+str(colToReplace)+"}")
                    execOnRowsDic[i] = newExcecFunction

            columnStr = ",".join(sourceColumnStr)
            sourceSql = '%s %s %s' %(preSql,columnStr,postsql)

        """ EXECUTING SOURCE QUERY """
        sourceSql = str(sourceSql) if self.isExtractSqlIsOnlySTR else sourceSql

        self.exeSQL(sql=sourceSql , commit=False)
        p("EXTRACTING SQL:\n %s" %sourceSql,"ii")

        if len(targetColumnStr) == 0:
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
        ## Compare existint target strucutre
        tarStrucutre = self.getStructure(tableName=self.connObj, tableSchema=self.defaultSchema, sqlQuery=None)
        tarStrucutreL= {x.replace(pre, "").replace(pos, "").lower(): x for x in tarStrucutre}

        removeCol = {}
        for i, col in enumerate (targetColumn):
            if col.replace(pre, "").replace(pos, "").lower() not in tarStrucutreL:
                removeCol[i] = col
        if len (removeCol)>0:
            for num in removeCol:
                p("COLUMN NUMBER %s, NAME: %s NOT EXISTS IN TARGET TABLE, IGNORE COLUMN" %(num,removeCol[num]), "w")
                targetColumn.remove(removeCol[num])

                for i, r in enumerate (rows):
                    rows[i] = list(r)
                    del rows[i][num]

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
            p(u"TYPE:%s, OBJCT:%s ERROR in cursor.executemany !!!!" % (self.conn, self.connObj), "e")
            p(u"ERROR QUERY:%s " % execQuery, "e")
            sampleRes = ['Null' if not r else "'%s'" % r for r in rows[0]]
            p(u"SAMPLE:%s " % u", ".join(sampleRes), "e")
            p(e, "e")
            if config.LOOP_ON_ERROR:
                iCnt = 0
                tCnt = len(rows)
                errDict = {}
                totalErrorToLooap = int(tCnt * 0.1)
                totalErrorsFound = 0
                p("ROW BY ROW ERROR-> LOADING %s OUT OF %s ROWS " % (str(totalErrorToLooap), str(tCnt)),
                  "e")
                for r in rows:
                    try:
                        iCnt += 1
                        r = [r]
                        self.cursor.executemany(execQuery, r)
                        self.connDB.commit()
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
                            p("ROW BY ROW ERROR-> %s" %execQuery, "e")
                            p(ret, "e")
                            p(e, "e")
                        else:
                            errDict[errMsg] += 1

                p("ROW BY ROW ERROR-> LOADED %s OUT OF %s ROWS" % (str(totalErrorToLooap), str(tCnt)), "e")
                for err in errDict:
                    p("TOTAL ERRORS: %s, MSG: %s: " % (str(err), str(errDict[err])), "e")

    def execMethod(self, method=None):
        method = method if method else self.connObj

        if method and len(method)>0:
            p("CONN:%s, EXEC METHOD:\n%s" %(self.conn, method), "i")
            methodTup = [(1,method,{})]
            execQuery(sqlWithParamList=method, connObj=self, sqlFolder=self.sqlFolder)

    def merge (self, mergeTable, mergeKeys=None, sourceTable=None):
        srcSchema, srcName = self.setTableAndSchema(tableName=sourceTable, tableSchema=None, wrapTable=True)
        mrgSchema, mrgName = self.setTableAndSchema(tableName=mergeTable, tableSchema=None, wrapTable=True)

        srcStructure    = self.getDBStructure(tableName=srcName, tableSchema=srcSchema)
        mrgStructure    = self.getDBStructure(tableName=mrgName, tableSchema=mrgSchema)

        mrgStructureL   = {x.lower():x for x in mrgStructure}
        srcStructureL   = {x.lower():x for x in srcStructure}
        mergeKeysL      = [self.wrapColName(col=x.lower(), remove=False) for x in mergeKeys]

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

    def cntRows (self, objName=None):
        raise NotImplementedError("count rows need to be implemented")

    def createFrom (self, stt=None, objName=None, addIndex=None):
        if not self.creeateFromObjName:
            p ("CONN %s DO NOT HAVE TABLE TO CREATE FROM, IGNORE" %(self.conn), "e")
            return

        tableSchema, tableName = self.setTableAndSchema (tableName=self.creeateFromObjName )
        if self.isExists(tableName=tableName, tableSchema=tableSchema):
            sql = setSqlQuery().getSql(conn=self.conn, sqlType=eSql.CREATE_FROM, tableName=self.creeateFromObjName)
            self.exeSQL(sql=sql)

            rows = self.cursor.fetchall()
            if not rows or (rows and len(rows) < 1):
                p("ERROR CREATE FROM TABLE %s, CONN: %s -> NO ROWS" %(self.creeateFromObjName, self.conn),"ii")
                return

            tableToCreate = {}
            for col in rows:
                if len(col)==1:
                    if col[0] not in tableToCreate:
                        tableToCreate[col[0].lower()]= [col[0],OrderedDict()]
                elif len(col)==2:
                    if col[0] not in tableToCreate:
                        tableToCreate[col[0].lower()] = [col[0], OrderedDict()]
                        tableToCreate[col[0].lower()][1][col[1]] = {}
                        tableToCreate[col[0].lower()][1][col[1]][eJson.jSttValues.TYPE] = self.defDataType
                    else:
                        tableToCreate[col[0].lower()][1][col[1]] = {}
                        tableToCreate[col[0].lower()][1][col[1]][eJson.jSttValues.TYPE] = self.defDataType
                elif len(col)>=3:
                    if col[0] not in tableToCreate:
                        tableToCreate[col[0].lower()]= [col[0],OrderedDict()]
                        tableToCreate[col[0].lower()][1][col[1]] = {}
                        tableToCreate[col[0].lower()][1][col[1]][eJson.jSttValues.TYPE] = col[2]
                    else:
                        tableToCreate[col[0].lower()][1][col[1]] = {}
                        tableToCreate[col[0].lower()][1][col[1]][eJson.jSttValues.TYPE] = col[2]


            tableFilter = objName if objName else self.connObj

            filteredTables = {}
            if tableFilter and len(tableFilter)>0 and tableFilter.lower() in tableToCreate:
                filteredTables[tableToCreate[tableFilter.lower()][0]] = tableToCreate[tableFilter.lower()][1]
            else:
                for t in tableToCreate:
                    filteredTables[ tableToCreate[t][0] ] = tableToCreate[t][1]

            stt = stt if stt else None
            for t in filteredTables:
                ## ADD STT TO NEW CREATE TABLES
                if stt:
                    createStt = {x.lower():x for x in filteredTables[t]}
                    for col in stt:
                        if col.lower() in createStt:
                            filteredTables[t][ createStt[col.lower()] ].extend( stt[col] )
                        else:
                            filteredTables[t][ col ] = stt[col]

                self.create(stt=filteredTables[t], objName=t, addIndex=addIndex)


    ########################################################################################################

    """ INTERNAL USED  """
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
            p(e, "e")
            p(u"ERROR SQL:\n%s " % (uniocdeStr(s)), "e")
            return False

    """ INTERNAL USED:
        Return tableSchema, tableName From table name and schema 
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

    """ INTERNAL USED:  Wrap Column with DB brackets. Sample SqlServer column: productID --> [productId] """
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

    """ INTERNAL USED:  Convert all paramters in config.QUERY_PARAMS into variable and add it into SQL QUERY """
    def setQueryWithParams(self, query, queryParams=None):
        if queryParams and config.QUERY_PARAMS:
            queryParams.update(config.QUERY_PARAMS)
        else:
            queryParams = config.QUERY_PARAMS
        qRet = u""
        if query and len(query) > 0:
            if isinstance(query, (list, tuple)):
                for q in query:
                    for param in queryParams:
                        q = self.__replaceStr(sString=q, findStr=param, repStr=queryParams[param], ignoreCase=True,addQuotes="'")
                    qRet += q + u" "
            else:
                for param in queryParams:
                    if param in query:
                        query = self.__replaceStr(sString=query, findStr=param, repStr=queryParams[param],ignoreCase=True, addQuotes="'")
                qRet += query
            if len(queryParams)>0:
                p("REPLACE PARAMS: %s" % (str(queryParams)), "ii")
        else:
            qRet = query
        return qRet

    """ INTERNAL USED:replace paramters in setQueryWithParams Metod """
    def __replaceStr(self, sString, findStr, repStr, ignoreCase=True, addQuotes=None):
        if addQuotes and isinstance(repStr, str):
            findStrWithQuotes = "%s%s%s" % (addQuotes, findStr, addQuotes)
            if findStrWithQuotes not in sString:
                repStr = "%s%s%s" % (addQuotes, repStr, addQuotes)

        if ignoreCase:
            pattern = re.compile(re.escape(findStr), re.IGNORECASE)
            res = pattern.sub(repStr, sString)
        else:
            res = sString.replace(findStr, repStr)
        return res

    """ INTERNAL USED """
    def __isColumnExists (self, colName, tarToSrc):
        for ind, col in enumerate (tarToSrc):
            if col.lower() == colName.lower():
                return ind
            elif eJson.jSttValues.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.jSttValues.SOURCE].lower() == col.lower():
                return ind

        p("COLUMN %s NOT FOUND IN MAPPING" %(colName))
        return None

    """ INTERNAL USED """
    def __mapObjectToFile (self):
        fullFilePath = None
        if self.sqlFullFile and os.path.isfile(self.sqlFullFile):
            fullFilePath = self.sqlFullFile
        elif self.sqlFolder and self.sqlFullFile and  os.path.isfile( os.path.join (self.sqlFolder,self.sqlFullFile)):
            fullFilePath = os.path.join (self.sqlFolder,self.sqlFullFile)
        elif self.sqlFolder and os.path.isfile(os.path.join(self.sqlFolder, self.connObj)):
            fullFilePath = os.path.join(self.sqlFolder, self.connObj)

        if fullFilePath:
            foundQuery= False
            allParams = []
            if '.sql' in self.connObj:
                self.connObj = fullFilePath
            else:
                with io.open(fullFilePath, 'r', encoding='utf8') as inp:
                    sqlScript = inp.readlines()
                    allQueries = qp.querySqriptParserIntoList(sqlScript, getPython=True, removeContent=True, dicProp=None)

                    for q in allQueries:
                        allParams.append(q[1])
                        if q[0] and q[0].lower() == str(self.connObj).lower():
                            p("USING %s AS SQL QUERY " %(q[0]),"ii")
                            self.connObj = q[1]
                            self.connIsSql = True
                            foundQuery = True
                            break
                    else:
                        p("USING %s DIRECTLY, NOT FOUND IN %s" %(self.connObj,fullFilePath), "ii")

    """ INTERNAL USED """
    def minValues (self, colToFilter=None, resolution=None, periods=None, startDate=None):
        raise NotImplementedError("minValues need to be implemented")