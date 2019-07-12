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

from __future__ import  (absolute_import, division, print_function)
__metaclass__ = type

import os
import sqlite3
from lib.dingDong.misc.logger import p

class sqlLite ():
    def __init__(self, connFile):
        self.db     = None
        self.cur    = None

        self.seqTable = 'seq'
        sqlCreateDB =   [   "create table "+ self.seqTable + " (tbl varchar(100), col varchar(100), sId bigint);",
                            "CREATE UNIQUE INDEX idx_tbl ON " + self.seqTable + " (tbl);"]

        if os.path.isfile(connFile):
            self.db = sqlite3.connect(connFile)
            self.cur = self.db.cursor()
            p("dbSqlLite->init: sqlLite DB is exists, file : %s" % str(connFile), "ii")
        else:
            self.db = sqlite3.connect(connFile)
            self.cur = self.db.cursor()
            self.__execStatement(sqlCreateDB)
            p("dbSqlLite->init: sqlLite DB NOT exists.. creating new one at file : %s" % str(connFile), "i")

    # Get sequence from seq tables
    def execSqlGetValue (self, tblName, colName):
        sql = "select sId from %s where tbl like '%s' and col like '%s';" %(str(self.seqTable), str(tblName), str(colName) )
        row = self.__execSelect(sql, onlyOne=True)
        if row and len(row) > 0:
            newSeq = str(row[0])
            p("dbSqlLite->execSqlGetValue: sqlLite sequence for table: %s, column %s exists, seq: %s... " % (str(tblName), str(colName), newSeq ), "ii")
            return newSeq

        p("dbSqlLite->execSqlGetValue: sqlLite sequence for table: %s, column %s NOT EXISTS... " % (str(tblName), str(colName)), "i")
        return None

    def execReplaceSql (self, tblName, colName, sID):
        sql = "REPLACE INTO %s (tbl, col, sId) VALUES ('%s', '%s', %s);" % (str(self.seqTable), str(tblName), str(colName), str(sID))
        p("dbSqlLite->execReplaceSql: insert or update table: %s, column: %s with new sequence: %s " %(str(tblName), str(colName), str(sID)) ,"ii")
        self.__execStatement(sql)

    def close (self):
        self.db.close()

    def __execStatement(self, sqlList):
        if isinstance(sqlList, (list, tuple)):
            for s in sqlList:
                self.cur.execute(s)
        else:
            self.cur.execute(sqlList)

        self.db.commit()

    def __execSelect(self, sql, onlyOne=False):
        if 'select' in sql.lower():
            self.cur.execute(sql)
            p('dbSqlLite->:__execSelect: SELECT statement ... sql: %s' % (sql), "ii")
        else:
            p('dbSqlLite->:__execSelect: Not a select statement ... sql: %s' %(sql),"ii")
            return
        if onlyOne:
            return self.cur.fetchone()
        return self.cur.fetchall()