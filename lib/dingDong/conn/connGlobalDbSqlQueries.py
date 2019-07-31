# -*- coding: utf-8 -*-
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

from dingDong.misc.enumsJson import getAllProp, eConn, eSql
from dingDong.misc.logger import p

""" Abstract Class: """
class baseSqlQuery (object):
    def __init__ (self):
        self.allConn    = getAllProp (obj=eConn)
        self.connQuery  = {}
        self.default    = None

    def initSqlDict (self, defSql=None, conn=None):
        defSql = defSql if defSql else self.default
        connList = [conn] if conn else list (self.connQuery.keys())
        for conn in connList:
            self.connQuery[conn] = defSql

    """ Return SQL Query by connection type (sql, oracle ...) and sqlType string:
        RENAME , DROP, TRUNCATE, STRUCTURE, MERGE, ISEXISTS     
    """
    def getSql(self, conn, sqlType, **args):
        self.default = None
        for c in self.allConn:
            self.connQuery[c] = None

        if eSql.RENAME      == sqlType: self.setSqlRename(**args)
        elif eSql.DROP      == sqlType: self.setSqlDrop(**args)
        elif eSql.TRUNCATE  == sqlType: self.setSqlTruncate(**args)
        elif eSql.STRUCTURE == sqlType: self.setSqlTableStructure(**args)
        elif eSql.MERGE     == sqlType: self.setSqlMerge(**args)
        elif eSql.ISEXISTS  == sqlType: self.setSqlIsExists(**args)
        elif eSql.DELETE    == sqlType: self.setSqlDelete(**args)
        else:
            p("baseConnDbSqlQueries->getSql: %s IS NOT DEFINED  !" % (sqlType.upper()), "e")
            return None

        if conn not in self.connQuery:
            p("baseConnDbSqlQueries->getSqlRename: %s SQL QUERY FOR CONNENTION %s NOT IMPLEMENTED !" % (sqlType.upper(),conn), "e")
            return None

        if not self.connQuery[conn]:
            p("baseConnDbSqlQueries->getSqlRename: %s SQL QUERY FOR CONNECTION %s USING DEFAULT SQL " % (sqlType.upper(),conn),"ii")
            return self.default

        return self.connQuery[conn]

    def setSqlRename(self, tableSchema, tableName, tableNewName):
        pass

    def setSqlDrop (self, tableName, tableSchema):
        pass

    def setSqlTruncate (self, tableName, tableSchema):
        pass

    def setSqlTableStructure(self, tableName, tableSchema):
        pass

    def setSqlMerge (self, dstTable, srcTable, mergeKeys, colList , colFullList):
        pass

    def setSqlIsExists(self, tableName, tableSchema):
        pass

    def setSqlDelete (self, sqlFilter, tableName, tableSchema):
        pass



class setSqlQuery (baseSqlQuery):
    def __init__ (self):
        baseSqlQuery.__init__(self)

    def setSqlRename (self, tableSchema, tableName, tableNewName):
        tableName = '%s.%s' %(tableSchema, tableName) if tableSchema else tableName
        self.default = "ALTER TABLE %s RENAME TO %s;" %(tableName, tableNewName)
        self.connQuery[eConn.NONO] = self.default
        self.connQuery[eConn.LITE] = self.default

        self.connQuery[eConn.SQLSERVER] = "EXEC sp_rename '%s','%s'" %(tableName, tableNewName)


    def setSqlDrop (self, tableName, tableSchema):
        tableName = '%s.%s' % (tableSchema, tableName) if tableSchema else tableName
        self.default = "drop table %s " %(tableName)
        self.connQuery[eConn.SQLSERVER] = self.default
        self.connQuery[eConn.ORACLE]    = self.default
        self.connQuery[eConn.LITE] = self.default

    def setSqlTruncate (self, tableName, tableSchema):
        fullTableName = '%s.%s' % (tableSchema, tableName) if tableSchema else tableName
        self.default = "truncate table %s;" %(fullTableName)
        self.connQuery[eConn.SQLSERVER] = self.default
        self.connQuery[eConn.ORACLE]    = self.default
        self.connQuery[eConn.LITE] = "DELETE FROM %s;" %(fullTableName)

    def setSqlTableStructure (self, tableName, tableSchema):

        self.default = "SQL Structure is not implemented ;"
        #### SQL SERVER
        sql = """
        SELECT c.name,
        UPPER(tp.name) +
        CASE	WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text')
                    THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
                WHEN tp.name IN ('nvarchar', 'nchar')
                    THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
                WHEN tp.name IN ('ntext')
                    THEN ''
                WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')
                    THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
                WHEN tp.name IN ('decimal','numeric')
                    THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'
            ELSE ''
            END as colType

        FROM sys.columns c WITH (NOWAIT)
        JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id
        WHERE c.[object_id] =
            (Select top 1 object_id as obID from
            (SELECT SCHEMA_NAME(schema_id) schemaDesc , name , object_id FROM sys.tables
             Union
            Select SCHEMA_NAME(schema_id) schemaDesc , name , object_id FROM sys.views) tt
            Where """
        sql += "schemaDesc='" + tableSchema.replace("[", "").replace("]", "") + "' and " if tableSchema else ""
        sql += "name='" + tableName.replace("[", "").replace("]", "") + "') ORDER BY c.column_id"

        self.connQuery[eConn.SQLSERVER] = sql
        self.connQuery[eConn.LITE]      = "select name, type from pragma_table_info('%s');" %tableName

        ### ORACLE
        sql = "select column_name, data_type || "
        sql += "case when data_type = 'NUMBER' and data_precision is not null then '(' | | data_precision | | ',' | | data_scale | | ')' "
        sql += "when data_type = 'NUMBER' then '(18,0)' "
        sql += "when data_type like '%CHAR%' then '(' | | data_length | | ')' "
        sql += "else '' end type "
        sql += "from all_tab_columns where table_name = '" + tableName + "' "
        sql += " and owner='" + tableSchema + "'" if tableSchema else ""
        sql += " ORDER BY COLUMN_ID"

        self.connQuery[eConn.ORACLE] = str(sql)

        ### MYSQL
        sql = """
        SELECT distinct column_name, column_type  FROM information_schema.columns
        WHERE table_name='""" + tableName + "' "
        sql += "and TABLE_SCHEMA='" + tableSchema + "';" if tableSchema else ";"

        self.connQuery[eConn.MYSQL] = str(sql)

        ### VERTICA
        sql = """
            SELECT distinct column_name, data_type  FROM columns
            WHERE table_name='""" + tableName + "' "
        sql += "and table_schema='" + tableSchema + "';" if tableSchema else ";"

        self.connQuery[eConn.VERTICA] = str(sql)

    def setSqlMerge (self, dstTable, srcTable, mergeKeys, colList , colFullList):
        ### SQL AND DEFAULT
        sql = "MERGE INTO " + dstTable + " as t USING " + srcTable + " as s ON ("
        colOnMerge = " AND ".join(["ISNULL (t." + c + ",'')= ISNULL (s." + c + ",'')" for c in mergeKeys])
        sql += colOnMerge + ") \n WHEN MATCHED THEN UPDATE SET \n"
        for c in colList:
            # Merge only is source is not null
            sql += "t." + c + "=" + "case when s." + c + " is null or len(s." + c + ")<1 then t." + c + " else s." + c + " End,\n"
        sql = sql[:-2] + "\n"
        sql += " WHEN NOT MATCHED THEN \n"
        sql += " INSERT (" + ",".join([c for c in colFullList]) + ") \n"
        sql += " VALUES  (" + ",".join(["s." + c for c in colFullList]) + "); "

        self.default = sql
        self.connQuery[eConn.SQLSERVER] = sql

    def setSqlIsExists(self, tableName, tableSchema):
        fullTableName = '%s.%s' %(tableSchema, tableName) if tableSchema else tableName
        sql = "Select OBJECT_ID('%s')" %(fullTableName)
        self.default = sql
        self.connQuery[eConn.SQLSERVER] = sql
        self.connQuery[eConn.LITE] = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = '%s';" %(fullTableName)



    def setSqlDelete (self, sqlFilter, tableName, tableSchema):
        fullTableName = '%s.%s' % (tableSchema, tableName) if tableSchema else tableName
        sql ="Delete From %s where %s " %(fullTableName, sqlFilter)
        self.default = sql
        self.connQuery[eConn.SQLSERVER] = sql

################################################################################################
###################   OLD VERSION - TAL 2019/05/29 --> NEED TO DELETE
################################################################################################

######### SQL : COLUMN NAMES : column name
def sql_columnsNames ( tbl , schema ):

    if schema:
        sql = "select column_name from information_schema.columns where table_name='" + tbl + "' And table_schema='" + schema + "' order by ordinal_position"
    else:
        sql = "select column_name from information_schema.columns where table_name='" + tbl + "' order by ordinal_position"
    return sql

def oracle_columnsNames (tblName, tblSchema=None):
    if not tblSchema:
        endSchema = tblName.find(".", 0)
        if endSchema > 0:
            tblSchema = tblName[0:endSchema]
            tblName = tblName[endSchema + 1:]
    if tblSchema:
        sql = "select column_name from information_schema.columns where table_name='" + tblName + "' And table_schema='" + tblSchema + "' order by ordinal_position"
    else:
        sql = "select column_name from information_schema.columns where table_name='" + tblName + "' order by ordinal_position"
    return sql

######### SQL : DATABASE STRUCURE
def sql_objectStrucute (filterDic):
    typesObj    = "'BASE TABLE','VIEW'"
    likeStr     = None
    sql         = ""
    if filterDic:
        if 'type' in filterDic:
            typesObj = ",".join([ "'"+x.replace ('"','').replace("'","")+"'" for x in filterDic['type']])
        if 'like' in filterDic:
            likeStr = " TABLE_NAME like ('%"+filterDic['like'].replace("'","").replace('"','')+"%') "
    # select type_desc,type, name from sys.objects WHERE type in ( %s ) AND %s order by name
    # SELECT TABLE_SCHEMA+'.'+TABLE_NAME FROM INFORMATION_SCHEMA.TABLES Where TABLE_TYPE in ('BASE TABLE','VIEW')
    if  likeStr:
        sql = "SELECT TABLE_SCHEMA+'.'+TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES Where TABLE_TYPE in ( %s ) AND %s " %( typesObj , likeStr)
    else:
        sql = "SELECT TABLE_SCHEMA+'.'+TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES Where TABLE_TYPE in ( %s ) " % (typesObj)
    sql+=" ORDER BY TABLE_SCHEMA+'.'+TABLE_NAME"
    return sql

######### SQL : MINIMUN VALUE : tblName, tblSchema, resolution, periods, col=None, startDate=None
def sql_minValue (tblName, tblSchema, resolution,periods,col=None, startDate=None):
    sql = ""
    dDate = " getdate() "
    if startDate:
        dDate = (" Convert (smalldatetime '%s') " %str(startDate))
    if col:
        sql = "Select CONVERT (DATE, MIN (%s)) FROM " % str (col)
        if tblSchema:
            sql += tblSchema + "." + tblName
        else:
            sql += tblName
    else:
        sql = "Select convert (date, dataadd (%s, %s, %s))" %(str(resolution),str(periods),dDate)
    return sql

def mysql_minValue (tblName, tblSchema, resolution,periods,col=None, startDate=None):
    sql = ""
    if 'd' == resolution: resolution = "DAY"
    if 'm' == resolution: resolution = "MONTH"
    if 'y' == resolution: resolution = "YEAR"

    dDate = " CURDATE() "
    if startDate:
        dDate = (" '%s' " %str(startDate))
    if col:
        sql = "Select DATE ( MIN (%s)) FROM " % str (col)
        if tblSchema:
            sql += tblSchema + "." + tblName
        else:
            sql += tblName
    else:
        sql = "Select DATE ( DATE_ADD(%s, INTERVAL %s %s))" %(dDate,str(periods),str(resolution))
    return sql

def oracle_minValue (tblName, tblSchema, resolution,periods,col=None, startDate=None ):
    sql = ""
    dDate = " getdate() "
    if startDate:
        dDate = (" TO_DATE ('%s') " % str(startDate))
    if col:
        sql = "Select trunc (MIN (%s) ) FROM " % str (col)
        if tblSchema:
            sql += "`"+tblSchema + "`" + "." + "`" + tblName + "`"
        else:
            sql += "`" + tblName + "`"
    if 'd' in resolution:
        sql = 'Select trunc (%s+%s) from dual;' %(dDate, str(periods))
    if 'm' in resolution:
        sql =  'Select trunc (add_months(%s, %s)) from dual;' %(dDate, str(periods))
    if 'y' in resolution:
        sql =  'Select trunc (add_months(%s, %s)) from dual;' %(dDate, str(periods*12))
    return sql

######### SQL : SEQUENCE : column, type, start, leg
def sql_seq (seqDic):
    sql = ""
    if 'column' in seqDic and 'type' in seqDic and 'start' in seqDic and 'inc' in seqDic:
        if 'merge' in seqDic:
            # "["+seqDic['column']+"]"+"\t"+
            sql = "["+seqDic['type']+"],\n"
        else:
            sql = "["+seqDic['type']+"]"+"\t"+" IDENTITY("+str(seqDic['start'])+","+str(seqDic['inc'])+") NOT NULL,\n"
    return sql