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
import pymongo

from dingDong.conn.connGlobalDB   import baseGlobalDb
from dingDong.misc.enumsJson      import eConn, eJson, findProp

DEFAULTS    = { eJson.jValues.DEFAULT_TYPE: 'string', eJson.jValues.SCHEMA: None,
                eJson.jValues.EMPTY: 'null', eJson.jValues.COLFRAME: ("", ""), eJson.jValues.SP: {}}

DATA_TYPES  = { eConn.dataType.DB_VARCHAR:['string', 'regex', 'array', 'ntext'],
                    eConn.dataType.DB_INT:['int', 'long'],
                    eConn.dataType.DB_FLOAT:['double'],
                    eConn.dataType.DB_DATE:['date','timestamp']
                    }


class mongo (baseGlobalDb):
    def __init__ (self, connPropDict=None, conn=None, connUrl=None, connExtraUrl=None,
                  connName=None,connObj=None,  connFilter=None, connIsTar=None,
                  connIsSrc=None, connIsSql=None):

        baseGlobalDb.__init__(self, connPropDict=connPropDict, conn=conn, connUrl=connUrl, connExtraUrl=connExtraUrl,
                                    connName=connName, connObj=connObj, connFilter=connFilter,
                                    connIsTar=connIsTar, connIsSrc=connIsSrc, connIsSql=connIsSql)

    def connect(self):
        self.connDB = pymongo.MongoClient(self.connUrl)
        self.cursor = self.connDB[self.conn]



    def getStructure(self, tableName=None, tableSchema=None, sqlQuery=None):
        pass




