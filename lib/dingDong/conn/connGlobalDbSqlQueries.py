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
        elif eSql.TABLE_COPY_BY_COLUMN    == sqlType: self.tblCopyByColumn(**args)

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

    def tblCopyByColumn(self, tableName, tableSchema, srcTableName, columns):
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

    def tblCopyByColumn(self, tableName, tableSchema, srcTableName, columns):
        sourceTableName = '%s.%s' % (tableSchema, tableName) if tableSchema else srcTableName
        targetTableName = '%s.%s' % (tableSchema, tableName) if tableSchema else tableName
        columns= ",".join(columns)

        sql = "insert into %s (%s) select %s from %s" % (targetTableName,columns,columns, sourceTableName)
        self.default = sql
        self.connQuery[eConn.SQLSERVER] = sql