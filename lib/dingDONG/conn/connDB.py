# -*- coding: utf-8 -*-
# (c) 2017-2020, Tal Shany <tal.shany@biSkilled.com>
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
from collections import OrderedDict

from dingDONG.conn.baseConnBatch import baseConnBatch
from dingDONG.conn.transformMethods import *
from dingDONG.misc.enums            import eConn, eSql, eJson, eObj
from dingDONG.misc.globalMethods import uniocdeStr, setProperty
from dingDONG.config                import config
from dingDONG.misc.logger           import p

import dingDONG.conn.connDBParser as qp
from dingDONG.conn.connDBQueries import setSqlQuery
from dingDONG.executers.executeSql import execQuery

try:
    import cx_Oracle  # version : 6.1
except ImportError:
    p("cx_Oracle is not installed", "ii")

DEFAULTS = {
            eConn.types.NONO: {   eConn.defaults.DEFAULT_TYPE:'varchar(100)',eConn.defaults.TABLE_SCHEMA:'dbo',
                                  eConn.defaults.COLUMNS_NULL:'Null', eConn.defaults.COLUMN_FRAME:("[","]"),
                                  eConn.defaults.SP:{'match':None, 'replace':None}, eConn.defaults.UPDATABLE:False},

            eConn.types.ORACLE: {eConn.defaults.DEFAULT_TYPE: 'varchar(100)', eConn.defaults.TABLE_SCHEMA: 'dbo',
                                 eConn.defaults.COLUMNS_NULL: 'Null', eConn.defaults.COLUMN_FRAME: ('"', '"'),
                                 eConn.defaults.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}, eConn.defaults.UPDATABLE:True},

            eConn.types.SQLSERVER: {eConn.defaults.DEFAULT_TYPE: 'varchar(100)', eConn.defaults.TABLE_SCHEMA: 'dbo',
                                    eConn.defaults.COLUMNS_NULL: 'Null', eConn.defaults.COLUMN_FRAME: ("[", "]"),
                                    eConn.defaults.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}, eConn.defaults.UPDATABLE:True  },

            eConn.types.POSTGESQL: {eConn.defaults.DEFAULT_TYPE: 'varchar(100)', eConn.defaults.TABLE_SCHEMA: 'public',
                                    eConn.defaults.COLUMNS_NULL: 'Null', eConn.defaults.COLUMN_FRAME: ('"', '"'),
                                    eConn.defaults.SP: {'match': r'([@].*[=])(.*?(;|$))', 'replace': r"[=;@\s']"}, eConn.defaults.UPDATABLE:True  },

            eConn.types.LITE: {   eConn.defaults.DEFAULT_TYPE:'varchar(100)',eConn.defaults.TABLE_SCHEMA:None,
                                  eConn.defaults.COLUMNS_NULL:'Null', eConn.defaults.UPDATABLE:eConn.updateMethod.DROP}
           }

DATA_TYPES = {
    eConn.dataTypes.B_STR: {
                            eConn.dataTypes.DB_VARCHAR:None,
                            eConn.dataTypes.DB_NVARCHAR:None,
                            eConn.dataTypes.DB_CHAR:None,
                            eConn.dataTypes.DB_BLOB:None},
    eConn.dataTypes.B_INT: {eConn.dataTypes.DB_INT:None,
                           eConn.dataTypes.DB_BIGINT:None},
    eConn.dataTypes.B_FLOAT:{eConn.dataTypes.DB_FLOAT:None,
                            eConn.dataTypes.DB_DECIMAL:None},
    eConn.dataTypes.DB_DATE:{eConn.dataTypes.DB_DATE:None}
}

EXTEND_DATA_TYPES = {
    eConn.types.ORACLE:   { eConn.dataTypes.DB_DATE:['date','datetime'],
                      eConn.dataTypes.DB_VARCHAR:['varchar','varchar2'],
                      eConn.dataTypes.DB_DECIMAL:['number','numeric','dec','decimal']
                    },
    eConn.types.SQLSERVER:{
                        eConn.dataTypes.DB_DATE:['smalldatetime','datetime'],
                        eConn.dataTypes.DB_DECIMAL:['decimal']
                    },
    eConn.types.POSTGESQL:{
                        eConn.dataTypes.DB_DATE:['smalldatetime','datetime'],
                        eConn.dataTypes.DB_DECIMAL:['decimal']
                    },
    eConn.types.ACCESS: { eConn.dataTypes.DB_VARCHAR:['varchar', 'longchar', 'bit', 'ntext'],
                    eConn.dataTypes.DB_INT:['integer', 'counter'],
                    eConn.dataTypes.DB_FLOAT:['double'],
                    eConn.dataTypes.DB_DECIMAL:['decimal']
                    }
}

class connDb (baseConnBatch):
    def __init__ (self, propertyDict=None, connType=None, connName=None, connIsTar=None,
                  connIsSrc=None, connIsSql=None, connUrl=None, connTbl=None,  connFilter=None,
                  defaults=None, dataTypes=None):

        baseConnBatch.__init__(self, propertyDict=propertyDict, connType=connType, connName=connName,
                                connIsTar=connIsTar,connIsSrc=connIsSrc, connIsSql=connIsSql,
                               connUrl=connUrl, connTbl=connTbl, connFilter=connFilter,defaults=DEFAULTS[eConn.types.NONO], dataTypes=DATA_TYPES)

        if defaults:
            self.defaults.update(defaults)

        if self.connType in DEFAULTS:
            self.defaults.update(DEFAULTS[self.connType])

        if self.connType in EXTEND_DATA_TYPES:
            self.dataTypes = self.setDataTypes(connDataTypes=EXTEND_DATA_TYPES[self.connType]).copy()

        if dataTypes:
            self.dataTypes = self.setDataTypes(connDataTypes=dataTypes)

        self.defDataType        = self.defaults[eConn.defaults.DEFAULT_TYPE]
        self.update             = self.defaults[eConn.defaults.UPDATABLE]

        """ DB PROPERTIES """
        self.connUrl    = setProperty(k=eConn.props.URL, o=self.propertyDict, defVal=self.propertyDict)
        self.connTbl    = setProperty(k=eConn.props.TBL, o=self.propertyDict, defVal=None)
        self.sqlFolder  = setProperty(k=eConn.props.FOLDER, o=self.propertyDict, defVal=None)
        self.sqlFile    = setProperty(k=eConn.props.SQL_FILE, o=self.propertyDict, defVal=None)

        self.defaultSchema  = self.defaults[eConn.defaults.TABLE_SCHEMA]
        self.defaulNull     = self.defaults[eConn.defaults.COLUMNS_NULL]
        self.defaultSP      = self.defaults[eConn.defaults.SP]
        self.columnFrame    = self.defaults[eConn.defaults.COLUMN_FRAME]

        self.cursor         = None
        self.connDB         = None
        self.connSql        = None

        self.isExtractSqlIsOnlySTR  = False

        if self.connIsSql:
            self.connSql    = self.setQueryWithParams(self.connTbl)
            self.connTbl    = self.connSql

        elif not self.connTbl or (self.connTbl and (self.connTbl =="*" or self.connTbl =="")):
            self.isSingleObject = False

        elif self.connTbl and len(self.connTbl)>0 \
                and ('.sql' not in self.connTbl and (not self.sqlFile or (self.sqlFile and self.sqlFile not in self.connTbl))):

            self.connSql = "SELECT * FROM %s" %self.connTbl

            if self.connIsSrc or self.connIsTar:
                self.connTbl        = self.wrapColName(col=self.connTbl, remove=True).split(".")
                self.defaultSchema  = self.connTbl[0] if len(self.connTbl) > 1 else self.defaultSchema
                self.connTbl        = self.connTbl[1] if len(self.connTbl) > 1 else self.connTbl[0]

            if self.connFilter and len(self.connFilter) > 1:
                self.connFilter = re.sub(r'WHERE', '', self.connFilter, flags=re.IGNORECASE)
                self.connSql = '%s WHERE %s' %(self.connSql, self.setQueryWithParams(self.connFilter))

            self.objNames[self.connTbl] = {eObj.DB_TBL_SCHEMA:self.defaultSchema, eObj.DB_QUERY:self.connSql}

        self.__mapObjectToFile()
        objName = "QUERY " if self.connIsSql else "TABLE: %s" %self.connTbl

        self.versionManager = None
        self.connect()

    def connect(self):
        odbc = None
        try:
            if eConn.types.MYSQL == self.connType:
                import pymysql
                self.connDB = pymysql.connect(self.connUrl[eConn.connString.URL_HOST], self.connUrl[eConn.connString.URL_USER],
                                              self.connUrl[eConn.connString.URL_PASS], self.connUrl[eConn.connString.URL_DB])
                self.cursor = self.connDB.cursor()

            elif eConn.types.POSTGESQL == self.connType:
                import psycopg2
                self.connDB = psycopg2.connect(self.connUrl)
                self.cursor = self.connDB.cursor()

            elif eConn.types.VERTICA == self.connType:
                import vertica_python
                self.connDB = vertica_python.connect(self.connUrl)
                self.cursor = self.connDB.cursor()

            elif eConn.types.ORACLE == self.connType:
                self.isExtractSqlIsOnlySTR = True

                self.connDB = cx_Oracle.connect(self.connUrl[eConn.connString.URL_USER], self.connUrl[eConn.connString.URL_PASS], self.connUrl[eConn.connString.URL_DSN])
                if 'nls' in self.connUrl:
                    os.environ["NLS_LANG"] = self.connUrl[eConn.connString.URL_NLS]
                self.cursor = self.connDB.cursor()
            elif eConn.types.ACCESS == self.connType:
                import pyodbc as odbc
                self.connDB = odbc.connect(self.connUrl)  # , ansi=True
                self.cursor = self.connType.cursor()
                self.cColoumnAs = False
            elif eConn.types.LITE == self.connType:
                import sqlite3 as sqlite
                self.connDB = sqlite.connect(self.connUrl)  # , ansi=True
                self.cursor = self.connDB.cursor()
            elif eConn.types.SQLSERVER == self.connType:
                try:
                    if eConn.props.DB_PYODBC in self.propertyDict:
                        import pyodbc as odbc
                    else:
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
            else:
                import pyodbc as odbc
                self.connDB = odbc.connect(self.connUrl)  # ansi=True
                self.cursor = self.connDB.cursor()
                p("CONN %s is not defined, using PYODBC connection " %self.connType, "w")

            p("CONNECTED, DB TYPE: %s, URL: %s" % (self.connType, self.connUrl), "ii")
            if not self.isSingleObject:
                sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.ALL_TABLES, filterTables=self.connFilter)
                self.exeSQL(sql, commit=False)
                rows = self.cursor.fetchall()
                if rows and len(rows) > 0:
                    for col in rows:
                        tableSchema, tableName = self.setTableAndSchema(tableName=col[0])
                        self.objNames[tableName] = {eObj.DB_TBL_SCHEMA: tableSchema,
                                                    eObj.DB_QUERY: """Select * From %s""" % col[0]}
                    p("There are %s Tables to use" % (str(len(rows))), "ii")

            return True

        except ImportError:
            p("%s is not installed" % (self.connType), "e")
        except Exception as e:
            err = "Error connecting into DB: %s, ERROR: %s\n " % (self.connType, str(e))
            err += "USING URL: %s\n" %(self.connUrl)
            p (err,"e")
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
        baseConnBatch.test(self)

    def isExists(self, tableName, tableSchema=None):
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=False)
        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.ISEXISTS, tableName=tableName, tableSchema=tableSchema)
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        if row and row[0]:
            return True
        p("SCHEMA:%s, TABLE:%s NOT EXISTS" % (tableSchema, tableName), "ii")
        return False

    def __create (self, stt, tableName, tableSchema=None, addIndex=None):
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=False)
        tableFullName = '%s.%s' % (tableSchema, tableName) if tableSchema else tableName

        isNew, isChanged, newHistoryTable = self.cloneObject(stt=stt, tableName=tableName)
        if isNew or (isChanged and (self.update in [eConn.updateMethod.UPDATE, eConn.updateMethod.DROP])):
            sql = "CREATE TABLE %s \n (" % (tableFullName)
            for col in stt:
                if eJson.stt.ALIACE in stt[col] and stt[col][eJson.stt.ALIACE] and len(stt[col][eJson.stt.ALIACE]) > 0:
                    colName = self.wrapColName(col=stt[col][eJson.stt.ALIACE], remove=False)
                else:
                    colName = self.wrapColName(col=col, remove=False)
                colType = stt[col][eJson.stt.TYPE]
                sql += '\t%s\t%s,\n' % (colName, colType)

            sql = sql[:-2] + ')'
            p("CREATE TABLE: \n" + sql)
            self.exeSQL(sql=sql, commit=True)
            if self.versionManager: self.versionManager(sql)

        # Check for index
        if addIndex and self.update != eConn.updateMethod.NO_UPDATE:
            self.addIndexToTable(tableName, addIndex)

        if config.DING_ADD_OBJECT_DATA:
            if newHistoryTable and len(newHistoryTable) > 0:
                columns = []
                pre, pos = self.columnFrame[0], self.columnFrame[1]
                oldStructure = self.getStructure(tableName=newHistoryTable, sqlQuery=None)
                newStructure = self.getStructure(tableName=tableName, sqlQuery=None)

                oldStructureL = {x.replace(pre, "").replace(pos, "").lower(): x for x in oldStructure}
                newStructureL = {x.replace(pre, "").replace(pos, "").lower(): x for x in newStructure}

                for col in oldStructureL:
                    if col in newStructureL:
                        columns.append(self.wrapColName(col, remove=False))

                if len(columns) > 0:
                    sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.TABLE_COPY_BY_COLUMN,
                                               tableName=tableName, tableSchema=tableSchema,
                                               srcTableName=newHistoryTable, columns=columns)
                    self.exeSQL(sql=sql)
                    p("ADD ROWS TO %s FROM %s, UPDATED COLUMNS:\n%s" % (tableName, newHistoryTable, ",".join(columns)),
                      "w")

    def create(self, sttDict=None, addIndex=None):
        sttDict = sttDict if sttDict else self.objNames

        if not sttDict or len (sttDict)==0:
            p("TABLE %s NOT MAPPED CORRECLTY " %(self.connTbl), "e")
            return

        if self.isSingleObject:
            self.__create(stt=sttDict, tableName=self.connTbl, tableSchema=None, addIndex=addIndex)

        else:
            for sttKey in sttDict:
                stt = sttDict[sttKey]
                self.__create(stt=stt, tableName=sttKey, tableSchema=None, addIndex=addIndex)


    """ INTERNAL USED: for create method: Compare existing stucture to new one, if 
            object exists call compareExistToNew - compare 2 object and update   """
    def cloneObject(self, stt, tableName):
        newStructureL   = OrderedDict()
        isNew           = True
        isChanged       = False

        pre, pos = self.columnFrame[0], self.columnFrame[1]

        for col in stt:
            colAlias = stt[col][eJson.stt.ALIACE] if eJson.stt.ALIACE in stt[col] else None
            colType  = stt[col][eJson.stt.TYPE] if eJson.stt.TYPE in stt[col] else self.defDataType
            if colAlias:
                newStructureL[colAlias.replace(pre, "").replace(pos, "").lower()] = (colAlias, colType)
            else:
                newStructureL[col.replace(pre, "").replace(pos, "").lower()] = (col, colType)

        existStructure = self.getStructure(tableName=tableName, sqlQuery=None)
        existStructure = next(iter(existStructure.values())) if len(existStructure)==1 else existStructure

        if not existStructure or len(existStructure) == 0:
            p("TABLE %s NOT EXISTS " % (tableName), "ii")
            return isNew, isChanged, None

        isNew = False
        isChanged, newHistoryTable = self.compareExistToNew(existStructure, newStructureL, tableName)
        return isNew, isChanged, newHistoryTable

    """ Strucutre Dictinary: {Column Name: {ColumnType:XXXXX } .... } 
        Help methods: getQueryStructure (baseConnDB), getDBStructure (baseConnDB)"""
    def getStructure(self, objects=None, tableName=None, sqlQuery=None):


        sqlQuery        = sqlQuery if sqlQuery else self.connSql
        objDict         = objects if objects else self.objNames

        retDicStructure = OrderedDict()

        # If there is query and there is internal maaping in query - will add this mapping to mappingColum dictionary
        if self.connIsSql:
            return self.getQueryStructure(sqlQuery=sqlQuery)

        elif tableName and len(tableName) > 0:
            tableSchema, tableName  = self.setTableAndSchema(tableName=tableName)
            return self.getDBStructure(tableName=tableName, tableSchema=tableSchema)
        elif self.isSingleObject:
            tableSchema, tableName = self.setTableAndSchema(tableName=self.connTbl)
            return self.getDBStructure(tableName=tableName, tableSchema=tableSchema)

        elif objDict and len (objDict)>0:
            if not isinstance(objDict, (dict, OrderedDict)):
                if objects in self.objNames:
                    tableSchema, tableName = self.setTableAndSchema(tableName=objects)
                    return self.getDBStructure(tableName=tableName, tableSchema=tableSchema)
                else:
                    p("TABLE %s IS NOT EXISTS " % str(objects))
                    return OrderedDict()

            for table in objDict:
                tableSchema = objDict[table][eObj.DB_TBL_SCHEMA] if eObj.DB_TBL_SCHEMA in objDict[table] else None
                tableSchema, tableName = self.setTableAndSchema(tableName=table, tableSchema=tableSchema)
                tableFullName = '%s.%s' % (tableSchema, tableName) if tableSchema else tableName
                retDicStructure[tableFullName] = self.getDBStructure(tableName=tableName, tableSchema=tableSchema)

        return retDicStructure

    """ INTERNAL USED: TABLE STRUCTURE : {ColumnName:{Type:ColumnType, ALIACE: ColumnName} .... } """
    def getDBStructure(self, tableName, tableSchema):
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=False)
        ret = OrderedDict()

        if not self.isExists(tableName=tableName, tableSchema=tableSchema):
            p ("%s: TABLE %s, SCHEMA %s NOT EXISTS.. " %(self.connType, tableName, tableSchema), "ii")
            return ret

        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.STRUCTURE, tableName=tableName, tableSchema=tableSchema)
        self.exeSQL(sql, commit=False)

        rows = self.cursor.fetchall()
        if not rows or (rows and len(rows) < 1):
            p("ERROR(no rows) CONN: %s RECEIVE DB STRACTURE: TABLE: %s, SCHEMA: %s" % (self.connType, tableName, tableSchema), "e")

        for col in rows:
            colName = uniocdeStr(col[0])
            colType = uniocdeStr(col[1])
            val = {eJson.stt.TYPE: colType, eJson.stt.ALIACE: None}
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
            tableDb     = queryTableAndColunDic[tbl][qp.TABLE_DB]
            tableColumns = queryTableAndColunDic[tbl][qp.TABLE_COLUMN]

            tableSchema = '%s.%s'%(tableDb, tableSchema) if tableDb else tableSchema if tableSchema else None

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
                            ret[colTarName] = {eJson.stt.SOURCE: tableColL[colTbl],
                                               eJson.stt.TYPE: tableStrucure[tableColL[colTbl]][eJson.stt.TYPE]}
                            isFound = True
                            break

                if not isFound:
                    p("COLUMN %s NOT FOUND IN TABLE %s USING DEFAULT %s" % (col, tbl, self.defDataType), "ii")
                    ret[colTarName] = {eJson.stt.SOURCE: colLName, eJson.stt.TYPE: self.defDataType}

            ## Search for Column that ther is no table mapping
            for col in notFoundColumns:
                isFound = False
                colTarName = col
                colSName = notFoundColumns[col][0].replace (pre, "").replace (pos,"")
                colSName2= notFoundColumns[col][0]
                colLName = notFoundColumns[col][1]
                for colTbl in tableColL:
                    if colTbl.lower() == colSName.lower():
                        isFound = True
                        ret[colTarName] = {eJson.stt.SOURCE: colLName,
                                           eJson.stt.TYPE: tableStrucure[tableColL[colTbl]][eJson.stt.TYPE]}
                        foundColumns.append(colLName)
                        break
                if not isFound:
                    for colTbl in tableColL:
                        if colTbl.lower() in colSName2.lower():
                            ret[colTarName] = {eJson.stt.SOURCE: colLName,
                                               eJson.stt.TYPE: tableStrucure[tableColL[colTbl]][eJson.stt.TYPE]}
                            foundColumns.append(colLName)
                            break

        for col in notFoundColumns:
            colTarName = col
            colSName = notFoundColumns[col][0]
            colLName = notFoundColumns[col][1]
            if colLName not in foundColumns:
                p("COLUMN %s NOT FOUND IN ANY TABLE, USING DEFAULT %s" % (colLName, self.defDataType), "ii")
                ret[colTarName] = {eJson.stt.SOURCE: colLName, eJson.stt.TYPE: self.defDataType}


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
    def compareExistToNew(self, existStructure, newStructureL, tableName):

        tableSchema, tableName = self.setTableAndSchema(tableName=tableName,  wrapTable=False)
        isChanged       = False
        newHistoryTable = None

        updateDesc = 'UPDATE' if self.update == eConn.updateMethod.UPDATE else 'DROP CREATE' if self.update == eConn.updateMethod.DROP else 'WARNIN, NO UPDATE'
        pre, pos = self.columnFrame[0], self.columnFrame[1]
        existStructureL = {x.replace(pre, "").replace(pos, "").lower(): x for x in existStructure}

        for col in existStructureL:
            if col in newStructureL:
                ## update column type
                if existStructure[existStructureL[col]][eJson.stt.TYPE].lower() != newStructureL[col][1].lower():
                    isChanged = True
                    if self.update == eConn.updateMethod.UPDATE:
                        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.COLUMN_UPDATE,
                                                   tableName=tableName, tableSchema=tableSchema, columnName=existStructureL[col],
                                                   columnType=newStructureL[col][1])
                        self.exeSQL(sql=sql)
                        if self.versionManager: self.versionManager(sql)
                    p("%s: CONN:%s, TABLE: %s, COLUMN %s, TYPE CHANGED, OLD: %s, NEW: %s" % (updateDesc, self.connType, tableName, col, existStructure[existStructureL[col]][eJson.stt.TYPE],newStructureL[col][1]), "w")

            ## REMOVE COLUMN
            else:
                isChanged = True
                if self.update == eConn.updateMethod.UPDATE:
                    sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.COLUMN_DELETE, tableName=tableName,
                                               tableSchema=tableSchema, columnName=existStructureL[col])
                    self.exeSQL(sql=sql)
                    if self.versionManager: self.versionManager(sql)
                p("CONN:%s, TABLE: %s, REMOVING COLUMN: %s " % (self.connType, tableName, col), "w")

        for col in newStructureL:
            # ADD COLUMN
            if col not in existStructureL:
                isChanged = True
                if self.update == eConn.updateMethod.UPDATE:
                    sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.COLUMN_ADD,
                                               tableName=tableName, tableSchema=tableSchema, columnName=newStructureL[col][0],
                                               columnType=newStructureL[col][1])
                    self.exeSQL(sql=sql)
                    if self.versionManager: self.versionManager(sql)

                p("CONN:%s, TABLE: %s, ADD COLUMN: %s " % (self.connType, tableName, newStructureL[col][0]), "w")

        if not isChanged:
            p("TABLE %s DID NOT CHANGED  >>>>>" % (tableName), "ii")
            return isChanged, newHistoryTable
        else:
            if self.update == eConn.updateMethod.NO_UPDATE:
                p("TABLE STRUCTURE CHANGED, UPDATE IS NOT ALLOWED, NO CHANGE", "w")
                return isChanged, newHistoryTable
            if self.update not in (eConn.updateMethod.DROP, eConn.updateMethod.UPDATE):
                p("UPDATE STATUS IS %s, NOT KNOWN STATUS, IGNORE !!!" %(str(self.update)))
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
                        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.RENAME, tableSchema=tableSchema,
                                                   tableName=tableName, tableNewName=newHistoryTable)

                        # sql = eval (self.objType+"_renameTable ("+self.objName+","+oldName+")")
                        p("RENAME TABLE SQL:%s" % (str(sql)), "w")
                        self.exeSQL(sql=sql, commit=True)
                else:
                    if existStructure and len(existStructure) > 0:
                        p("TABLE HISTORY IS OFF AND TABLE EXISTS, DROP -> CREATE TABLE %s IN NEW STRUCTURE... " % (
                            str(tableName)), "w")
                        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.DROP, tableName=tableName,
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
                if eConn.props.DB_INDEX_COLUMS in ind and ind[eConn.props.DB_INDEX_COLUMS]:
                    columns  = ind[eConn.props.DB_INDEX_COLUMS] if isinstance(ind[eConn.props.DB_INDEX_COLUMS], list) else [ind[eConn.props.DB_INDEX_COLUMS]]
                    columns  = [x.lower() for x in columns]
                    isCluster= ind[eConn.props.DB_INDEX_CLUSTER] if eConn.props.DB_INDEX_CLUSTER in ind and ind[eConn.props.DB_INDEX_CLUSTER] is not None else False
                    isUnique = ind[eConn.props.DB_INDEX_UNIQUE] if eConn.props.DB_INDEX_UNIQUE in ind and ind[eConn.props.DB_INDEX_UNIQUE] is not None else False
                    newIndexList.append ({eConn.props.DB_INDEX_COLUMS:columns, eConn.props.DB_INDEX_CLUSTER:isCluster, eConn.props.DB_INDEX_UNIQUE:isUnique})
                else:
                    p("NOT VALID INDEX %s , MUST HAVE COLUNs, IGNORING" %(str(ind)), "i")


            ## update newIndexDict from db.
            tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema)
            tableName = '%s.%s' %(tableSchema,tableName) if tableSchema else tableName
            # check if there is cluster index
            sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.INDEX_EXISTS, tableName=tableName)
            self.cursor.execute(sql)
            rows = self.cursor.fetchall()
            if rows and len (rows)>0:
                for row in rows:
                    indexName = row[0]
                    if indexName not in existIndexDict:
                        existIndexDict[indexName] = {eConn.props.DB_INDEX_COLUMS:[], eConn.props.DB_INDEX_CLUSTER:False,eConn.props.DB_INDEX_UNIQUE:False}

                    existIndexDict[indexName][eConn.props.DB_INDEX_CLUSTER] = True if row[2] or str(row[2]) == '1' else False
                    if existIndexDict[indexName][eConn.props.DB_INDEX_CLUSTER] == True:
                        isClusterExists = True
                    existIndexDict[indexName][eConn.props.DB_INDEX_UNIQUE] = True if row[3] or str(row[3])  == '1' else False
                    existIndexDict[indexName][eConn.props.DB_INDEX_COLUMS].append (str(row[1]).lower())

            ### Compare - Remove existing idential Indexes
            for eInd in existIndexDict:
                if existIndexDict[eInd] in newIndexList:
                    newIndexList.remove(existIndexDict[eInd])

            ### Compare index with existing indexes
            for ind in newIndexList:
                if ind[eConn.props.DB_INDEX_CLUSTER] == True and isClusterExists:
                    p("CLUSTERED INDEX ALREADY EXISTS, IGNORE NEW INDEX:%s" %(str(ind)), "w")
                    toCreate = False
                else:
                    newColumn = ind[eConn.props.DB_INDEX_COLUMS]

                    for eInd in existIndexDict:
                        eColumn = existIndexDict[eInd][eConn.props.DB_INDEX_COLUMS]

                        if set(eColumn) == set(newColumn):
                            msg = """
                                    INDEX ON COLUMN %s EXISTS, OLD IS_CLUSTER:%s,
                                    OLD IS_UNIQUE:%s, NEW IS_CLUSTER: %s,
                                    NEW IS_UNIQUE: %s, IGNORING...""" %(str(eColumn),
                                                                         str(existIndexDict[eInd][eConn.props.DB_INDEX_CLUSTER]),
                                                                         str(existIndexDict[eInd][eConn.props.DB_INDEX_UNIQUE]),
                                                                         str(ind[eConn.props.DB_INDEX_CLUSTER]),
                                                                         str(ind[eConn.props.DB_INDEX_UNIQUE]))
                            p(msg , "i")
                            toCreate = False
                    if toCreate:
                        columnsCreate   = ind[eConn.props.DB_INDEX_COLUMS]
                        isUnique        = ind[eConn.props.DB_INDEX_UNIQUE]
                        isCluster       = ind[eConn.props.DB_INDEX_CLUSTER]
                        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.INDEX, tableName=tableName, columns=columnsCreate, isCluster=isCluster, isUnique=isUnique)
                        self.exeSQL(sql=sql)
                        p("TYPE:%s, ADD INDEX: COLUNS:%s, CLUSTER:%s, UNIQUE:%s\n SQL: %s" % (self.connType, str(columnsCreate),str(isCluster),str(isUnique),str(sql)), "ii")

    def preLoading(self, dictObj=None, sqlFilter=None):
        sqlFilter = sqlFilter if sqlFilter else self.connFilter

        dictObj = dictObj if dictObj else self.objNames

        if self.isSingleObject:
            if sqlFilter and len(sqlFilter) > 0:
                self.delete(sqlFilter=sqlFilter, tableName=self.connTbl, tableSchema=self.defaultSchema)
            else:
                self.truncate(tableName=self.connTbl, tableSchema=self.defaultSchema)

        elif dictObj:
            for table in dictObj:
                tableName   = table
                tableSchema = dictObj[table][eObj.DB_TBL_SCHEMA] if eObj.DB_TBL_SCHEMA in dictObj[table]  else None


                if sqlFilter and len(sqlFilter) > 0:
                    self.delete(sqlFilter=sqlFilter, tableName=tableName, tableSchema=tableSchema)
                else:
                    self.truncate(tableName=tableName, tableSchema=tableSchema)


    """ INTERNAL USED: preLoading method """
    def truncate(self, tableName=None, tableSchema=None):
        tableSchema, tableName  = self.setTableAndSchema (tableName=tableName, tableSchema=tableSchema, wrapTable=True)
        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.TRUNCATE, tableName=tableName, tableSchema=tableSchema)
        self.exeSQL(sql=sql)
        p("TYPE:%s, TRUNCATE TABLE:%s" % (self.connType, tableName),"ii")

    """ INTERNAL USED: preLoading method """
    def delete (self, sqlFilter, tableName=None, tableSchema=None):
        tableSchema, tableName = self.setTableAndSchema(tableName=tableName, tableSchema=tableSchema, wrapTable=True)
        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.DELETE, tableName=tableName, tableSchema=tableSchema, sqlFilter=sqlFilter)
        self.exeSQL(sql=sql)
        p("TYPE:%s, DELETE FROM TABLE:%s, WHERE:%s" % (self.connType, self.connTbl, self.connFilter), "ii")

    def extract(self, tar, tarToSrcDict, batchRows=None):
        batchRows = batchRows if batchRows else self.batchSize
        fnOnRowsDic     = {}
        execOnRowsDic   = {}
        pre,pos         = self.columnFrame[0],self.columnFrame[1]
        sourceColumnStr = []
        targetColumnStr = []
        sourceSql       = self.connSql

        if self.isSingleObject:
            tarToSrcDict = tarToSrcDict['']

        ## There is Source And Target column mapping
        if tarToSrcDict and len (tarToSrcDict)>0:
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

            for i,col in  enumerate (tarToSrcDict):
                tarColumn = col.replace(pre, "").replace(pos, "")
                tarColumnName = '%s%s%s' % (pre, tarColumn, pos)
                if eJson.stt.SOURCE in tarToSrcDict[col] and tarToSrcDict[col][eJson.stt.SOURCE]:
                    srcColumnName = tarToSrcDict[col][eJson.stt.SOURCE].replace(pre, "").replace(pos, "").lower()
                    if srcColumnName in existingColumnsByTargetL:
                        srcColumnName = '%s As %s' % (existingColumnsByTargetL[srcColumnName], tarColumnName)
                    elif srcColumnName in existingColumnsL:
                        srcColumnName = '%s As %s' % (existingColumnsL[srcColumnName], tarColumnName)
                    elif srcColumnName in existingColumnsLFull:
                        srcColumnName = '%s As %s' % (existingColumnsLFull[srcColumnName], tarColumnName)

                    else:
                        p("%s: %s, SOURCE COLUMN LISTED IN STT NOT EXISTS IN SOURCE TABLE, IGNORE COLUMN !!!!, OBJECT:\n%s" % (self.connType, tarToSrc[col][eJson.stt.SOURCE], self.connTbl), "e")
                        continue
                elif tarColumn.lower() in existingColumnsL:
                    srcColumnName = '%s As %s' %(existingColumnsL[ tarColumn.lower() ], tarColumnName)
                else:
                    srcColumnName =  "'' As %s" %(tarColumnName)

                sourceColumnStr.append (srcColumnName)
                targetColumnStr.append (col)

                ### ADD FUNCTION
                if eJson.stt.FUNCTION in tarToSrcDict[col] and tarToSrcDict[col][eJson.stt.FUNCTION]:
                    fnc = eval(tarToSrcDict[col][eJson.stt.FUNCTION])
                    fnOnRowsDic[i] = fnc if isinstance(fnc, (list, tuple)) else [fnc]

                ### ADD EXECUTION FUNCTIONS
                elif eJson.stt.EXECFUNC in tarToSrcDict[col] and len(tarToSrcDict[col][eJson.stt.EXECFUNC])>0:
                    newExcecFunction = tarToSrcDict[col][eJson.stt.EXECFUNC]
                    regex   = r"(\{.*?\})"
                    matches = re.finditer(regex, tarToSrcDict[col][eJson.stt.EXECFUNC], re.MULTILINE | re.DOTALL)
                    for matchNum, match in enumerate(matches):
                        for groupNum in range(0, len(match.groups())):
                            colName = match.group(1)
                            if colName and len(colName)>0:
                                colToReplace = match.group(1).replace("{","").replace("}","")
                                colToReplace = self.__isColumnExists (colName=colToReplace, tarToSrc=tarToSrcDict)
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
            p("TYPE:%s, OBJECT:%s ERROR FATCHING DATA" % (self.connType, str(self.connTbl)), "e")
            p(str(e), "e")

    def load(self, rows, targetColumn, objectName=None):
        totalRows = len(rows) if rows else 0
        if totalRows == 0:
            p("THERE ARE NO ROWS")
            return

        if objectName and len (objectName)>0 and not self.isSingleObject:
            if objectName in self.objNames:
                tableSchema = self.objNames[objectName][eObj.DB_TBL_SCHEMA] if self.objNames[objectName][eObj.DB_TBL_SCHEMA] else None
                tableName   = objectName
                tblFullName = "%s.%s" % (tableSchema, objectName) if tableSchema else objectName
            else:
                p("TABLE %s NOT EXISTS !" %(objectName), "e")
                return
        else:
            tableName = self.connTbl
            tblFullName = "%s.%s" %(self.defaultSchema, tableName) if self.defaultSchema else tableName

        execQuery = "INSERT INTO %s" % (tblFullName)
        pre, pos = self.columnFrame[0], self.columnFrame[1]
        colList = []
        colInsert = []
        ## Compare existint target strucutre

        tarStrucutre = self.getStructure(tableName=tableName, sqlQuery=None)
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
            p('LOAD %s into target: %s >>>>>> ' % (str(totalRows), tableName), "ii")

        except Exception as e:
            p(u"TYPE:%s, OBJCT:%s ERROR in cursor.executemany !!!!" % (self.connType, self.connTbl), "e")
            p(u"ERROR QUERY:%s " % execQuery, "e")
            sampleRes = ['Null' if not r else "'%s'" % r for r in rows[0]]
            p(u"SAMPLE:%s " % u", ".join(sampleRes), "e")
            p(e, "e")
            if config.DONG_LOOP_ON_FAILED_BATCH:
                iCnt = 0
                tCnt = len(rows)
                totalErrorToLooap = int(tCnt * 0.01)
                totalErrorsFound = 0
                p("ROW BY ROW ERROR-> LOADING %s OUT OF %s ROWS " % (str(totalErrorToLooap), str(tCnt)),"e")
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
                        errMsg = str(e)
                        ret = ""
                        for col in r[0]:
                            if col is None: ret += "Null, "
                            else:           ret += "'%s'," % (col)
                        p("ROW BY ROW ERROR-> %s" %execQuery, "e")
                        p(ret, "e")
                        p(errMsg, "e")
                p("ROW BY ROW ERROR-> LOADED %s OUT OF %s ROWS" % (str(totalErrorToLooap), str(tCnt)), "e")

    def execMethod(self, method=None):
        method = method if method else self.connTbl

        if method and len(method)>0:
            p("CONN:%s, EXEC METHOD:\n%s" %(self.connType, method), "i")
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

        sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.MERGE, dstTable=dstTable, srcTable=srcTable, mergeKeys=keyColumns, colList=updateColumns, colFullList=allColumns)
        self.exeSQL(sql=sql)
        p("TYPE:%s, MERGE %s WITH %s, \n\t\tMERGE KEYS:%s" %(self.connType, srcTable, dstTable, str(keyColumns)), "ii")

    def cntRows (self, objName=None):
        raise NotImplementedError("count rows need to be implemented")

    def createFromDbStrucure (self, stt=None, objName=None, addIndex=None):
        if not self.creeateFromObjName:
            p ("CONN %s DO NOT HAVE TABLE TO CREATE FROM, IGNORE" %(self.connType), "e")
            return

        tableSchema, tableName = self.setTableAndSchema (tableName=self.creeateFromObjName )
        if self.isExists(tableName=tableName, tableSchema=tableSchema):
            sql = setSqlQuery().getSql(conn=self.connType, sqlType=eSql.CREATE_FROM, tableName=self.creeateFromObjName)
            self.exeSQL(sql=sql)

            rows = self.cursor.fetchall()
            if not rows or (rows and len(rows) < 1):
                p("ERROR CREATE FROM TABLE %s, CONN: %s -> NO ROWS" %(self.creeateFromObjName, self.connType),"ii")
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
                        tableToCreate[col[0].lower()][1][col[1]][eJson.stt.TYPE] = self.defDataType
                    else:
                        tableToCreate[col[0].lower()][1][col[1]] = {}
                        tableToCreate[col[0].lower()][1][col[1]][eJson.stt.TYPE] = self.defDataType
                elif len(col)>=3:
                    if col[0] not in tableToCreate:
                        tableToCreate[col[0].lower()]= [col[0],OrderedDict()]
                        tableToCreate[col[0].lower()][1][col[1]] = {}
                        tableToCreate[col[0].lower()][1][col[1]][eJson.stt.TYPE] = col[2]
                    else:
                        tableToCreate[col[0].lower()][1][col[1]] = {}
                        tableToCreate[col[0].lower()][1][col[1]][eJson.stt.TYPE] = col[2]


            tableFilter = objName if objName else self.connTbl

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
                self.cursor.execute(s)  # if 'ceodbc' in odbc.__name__.lower() else self.connType.execute(s)
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
        tableName   = tableName if tableName else self.connTbl

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
            res = pattern.sub(str(repStr), sString)
        else:
            res = sString.replace(findStr, str(repStr))
        return res

    """ INTERNAL USED """
    def __isColumnExists (self, colName, tarToSrc):
        for ind, col in enumerate (tarToSrc):
            if col.lower() == colName.lower():
                return ind
            elif eJson.stt.SOURCE in tarToSrc[col] and tarToSrc[col][eJson.stt.SOURCE].lower() == col.lower():
                return ind

        p("COLUMN %s NOT FOUND IN MAPPING" %(colName))
        return None

    """ INTERNAL USED """
    def __mapObjectToFile (self):
        fullFilePath = None
        if self.sqlFile and os.path.isfile(self.sqlFile):
            fullFilePath = self.sqlFile
        elif self.sqlFolder and self.sqlFile and  os.path.isfile( os.path.join (self.sqlFolder,self.sqlFile)):
            fullFilePath = os.path.join (self.sqlFolder,self.sqlFile)
        elif self.sqlFolder and os.path.isfile(os.path.join(self.sqlFolder, self.connTbl)):
            fullFilePath = os.path.join(self.sqlFolder, self.connTbl)

        if fullFilePath:
            foundQuery= False
            allParams = []
            if '.sql' in self.connTbl:
                self.connTbl = fullFilePath
            else:
                with io.open(fullFilePath, 'r', encoding='utf8') as inp:
                    sqlScript = inp.readlines()
                    allQueries = qp.querySqriptParserIntoList(sqlScript, getPython=True, removeContent=True, dicProp=None)

                    for q in allQueries:
                        allParams.append(q[1])
                        if q[0] and q[0].lower() == str(self.connTbl).lower():
                            p("USING %s AS SQL QUERY " %(q[0]),"ii")
                            self.connTbl = q[1]
                            self.connIsSql = True
                            self.connSql = q[1]
                            foundQuery = True
                            break
                    else:
                        p("USING %s DIRECTLY, NOT FOUND IN %s" %(self.connTbl,fullFilePath), "ii")

    """ INTERNAL USED """
    def minValues (self, colToFilter=None, resolution=None, periods=None, startDate=None):
        raise NotImplementedError("minValues need to be implemented")