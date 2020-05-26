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

from collections import OrderedDict

from dingDONG.conn.connDB   import connDb
from dingDONG.misc.enums     import eConn, eJson

DEFAULTS    = { eConn.defaults.DEFAULT_TYPE: 'varchar(100)', eConn.defaults.TABLE_SCHEMA: 'dbo',
                eConn.defaults.COLUMNS_NULL: 'Null', eConn.defaults.COLUMN_FRAME: ("[", "]"), eConn.defaults.SP: {}}

DATA_TYPES  = { eConn.dataTypes.DB_VARCHAR:['varchar', 'longchar', 'bit', 'ntext'],
                    eConn.dataTypes.DB_INT:['integer', 'counter'],
                    eConn.dataTypes.DB_FLOAT:['double'],
                    eConn.dataTypes.DB_DECIMAL:['decimal']
                    }


class access (connDb):
    def __init__ (self, propertyDict=None, connType=None, connName=None,
                  connIsTar=False, connIsSrc=False, connIsSql=False,
                  connUrl=None,  connTbl=None, connFilter=None):

        connDb.__init__(self, propertyDict=propertyDict,connType=connType,connName=connName,
                        connIsTar=connIsTar, connIsSrc=connIsSrc, connIsSql=connIsSql,
                        connUrl=connUrl, connTbl=connTbl, connFilter=connFilter,
                        defaults=DEFAULTS, dataTypes=DATA_TYPES
                                    )

    def getStructure(self, tableName=None, tableSchema=None, sqlQuery=None):
        return self.__getAccessStructure(tableSchema=tableSchema, tableName=tableName)


    """ INTERNAL METHOD  """
    """ INTERNAL USE for getStrucure methid return ACCESS Structure dictionary  """
    def __getAccessStructure (self,tableSchema, tableName):
        tableName = '%s.%s' %(tableSchema, tableName) if tableSchema else tableName
        ret = OrderedDict()

        if self.conn == eConn.types.ACCESS:
            for row in self.cursor.columns():
                val = {eJson.stt.TYPE:self.defDataType, eJson.stt.ALIACE:None}

                if len(row) > 3:
                    curTblName = row[2].encode("utf-8")
                    if curTblName == tableName:
                        colName = row.column_name.encode("utf-8")
                        colType = row.type_name.lower()
                        if colType in DATA_TYPES[eConn.types.ACCESS][eConn.dataTypes.DB_VARCHAR]:
                            colDefType = 'varchar(MAX)' if row.column_size > 4098 else 'varchar(%s)' % str(row.column_size)
                        elif colType in DATA_TYPES[eConn.types.ACCESS][eConn.dataTypes.DB_DECIMAL]:
                            colDefType = 'decimal(%s,%s)' % (str(row.column_size), str(row.decimal_digits))
                        else:
                            colDefType = colType

                        val[eJson.stt.TYPE] = colDefType

                        ret[colName] = val
        return ret


