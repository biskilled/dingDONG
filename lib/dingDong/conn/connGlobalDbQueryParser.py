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

import re
import sqlparse
from collections import OrderedDict
from sqlparse.sql       import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens    import Keyword, DML

from dingDong.misc.logger import p
from dingDong.misc.enumsJson import eJson


COLUMNS   = '~COLUMNS'          # --> Dictionary : {columnName:TargetName, ...}
TABLES     = '~TABLES'          # --> Dictionary : {tableName:[column Names ... , '':[Columns Names ....]]

DIC = {COLUMNS:{}, TABLES:{}}

TABLE_ALIAS = 'alias'
TABLE_SCHEMA= 'schema'
TABLE_NAME  = 'tbl'
TABLE_COLUMN= 'column'

COLUMN_NAME= 'name'
COLUMN_ALIAS='alias'
COLUMN_TABLE='tbl'
COLUMN_SCHEMA='sc'

## extract_tableAndColumns -->
##      Using extract_tables which return Dictionary : {'Columns': {ColName:ColTargetName....}, 'Tables':{tableName: [Columns ....]}}
##      Using extract_table_identifiers which return list of [(table aliace, table schema, table Name) ...]

def extract_tableAndColumns (sql, pre="[", pos="]"):
    columnInTablesDict  = {'':[]}
    tableDict           = {}
    # let's handle multiple statements in one sql string
    statement = sqlparse.parse(sql)[0]
    allColumns = extractSQLColumns(sqlStr=sql, pre=pre, pos=pos)

    allTables = None
    if statement.get_type() != 'UNKNOWN':
        stream = __extract_from_part(statement)
        allTables = __extract_table_identifiers(stream)

    for tb in allTables:
        tblName     = tb[TABLE_NAME] if tb[TABLE_NAME] else ''
        tblSchema   = tb[TABLE_SCHEMA]
        tblAlias    = tb[TABLE_ALIAS] if tb[TABLE_ALIAS] else None
        fullTable   = '%s.%s' %(tblSchema, tblName) if tblSchema else tblName

        tableDict[fullTable] = [tblName.lower()]

        if tblAlias:
            tableDict[fullTable].append (tblAlias.lower())

        if tblSchema and len(tblSchema)>0:
            tableDict[fullTable].append(tblSchema.lower())

        columnInTablesDict[fullTable] = []

    for col in allColumns:
        colName     = col[COLUMN_NAME]
        colSchema   = col[COLUMN_SCHEMA]
        colTable    = col[COLUMN_TABLE]
        colAlias    = col[COLUMN_ALIAS]
        inTable     = False

        for tbl in tableDict:
            if colTable and colTable.lower() in tableDict[tbl]:
                if colSchema and len(colSchema)>0:
                    if colSchema.lower() in tableDict[tbl]:
                        columnInTablesDict[tbl].append({eJson.jSttValues.SOURCE:colName, eJson.jSttValues.ALIACE:colAlias}  )
                        inTable = True
                        break
                else:
                    columnInTablesDict[tbl].append({eJson.jSttValues.SOURCE:colName, eJson.jSttValues.ALIACE:colAlias})
                    inTable = True
                    break
        if not inTable:
            columnInTablesDict[''].append({eJson.jSttValues.SOURCE:colName, eJson.jSttValues.ALIACE:colAlias})

    return columnInTablesDict

def extract_TargetColumn (sql, pre="[", pos="]"):
    retColumn = []
    allColumns = extractSQLColumns(sqlStr=sql, pre=pre, pos=pos)
    for col in allColumns:
        print (col)

def __extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                value = ( identifier.get_alias() , identifier._get_first_name(), identifier.get_real_name())
                value = [x.replace('"', '').replace("'", "") if x else None for x in  value]
                value[1] = None if value[1]==value[2] else value[1]

                ## internal Query
                if isinstance(identifier.token_first(), Parenthesis):
                    value[2] = ''

            yield {TABLE_ALIAS:value[0], TABLE_SCHEMA:value[1], TABLE_NAME:value[2]}

        elif isinstance(item, Identifier):
            value = (item.get_alias(), item._get_first_name(), item.get_real_name())
            value = [x.replace('"', '').replace("'", "") if x else None for x in value]
            value[1] = None if value[1] == value[2] else value[1]
            ## internal Query
            if isinstance(item.token_first(), Parenthesis):
                value[2] = ''

            yield {TABLE_ALIAS:value[0], TABLE_SCHEMA:value[1], TABLE_NAME:value[2]}

def __extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if item.is_group:
            for x in __extract_from_part(item):
                yield x
        if from_seen:
            if __is_subselect(item):
                for x in __extract_from_part(item):
                    yield x
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING']:
                from_seen = False
                StopIteration
            else:
                yield item
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def __is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def replaceSQLColumns (sqlStr, columnStr):
    sqlStr = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sqlStr)
    sqlStr = re.sub(r"\s+", " ",sqlStr)
    ## Search : Select [top xx] [col1, col2.....] from [tables...]
    regSql    = r"(.*select\s+(?:top\s+\d+){0,1})(.*?)(from\s+.*)"

    #sqlGrouping = re.search(regexSql, sqlStr, re.IGNORECASE | re.MULTILINE)
    sqlGrouping = re.match(regSql, sqlStr, re.IGNORECASE | re.MULTILINE | re.S)
    if sqlGrouping:
        return '%s %s %s' %(sqlGrouping.group(1), columnStr, sqlGrouping.group(3))

    return sqlStr


""" Tal: My new masterpiece .... recieving SQL string, extract all columns and split it into
    alias name, table name (if exists) and column name"""
def extractSQLColumns (sqlStr, pre="[", pos="]"):
    colList = []
    retColList = []
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sqlStr)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # split on blanks, parens and semicolons
    tokens = re.split(r"GO|;", q, re.IGNORECASE)

    for tok in tokens:
        ### 3 Gropus: Select .... , Colums ....., From .....
        column = re.search(r"(select\s+)(.*?)[\s\n\t](from\s+.*)", tok, re.IGNORECASE | re.MULTILINE)
        if column and len(column.groups())>0:
            colStr = column.group(2).strip()
            # remove TAB, NEW LINE OR top(...)
            colStr = re.sub(r"^\s*top\s*[0-9]*|\t|\n", "", colStr, re.IGNORECASE | re.MULTILINE)
            colParentesis   = []
            colName = ''
            sqlBruckets = False
            for i in colStr:
                if pre and pre == i:
                    sqlBruckets = True
                    colName += i
                elif pos and pos == i:
                    sqlBruckets = False
                    colName += i
                elif '(' == i and not sqlBruckets:
                    colParentesis.append('p')
                    colName += i
                elif ')' == i and not sqlBruckets:
                    colParentesis = colParentesis[:-1]
                    colName += i
                elif ',' == i and len(colParentesis)==0:
                    colList.append(colName.strip())
                    colName = ''
                else:
                    colName+=i
            if colName:
                colList.append(colName.strip())

    ## Extract All column
    for col in colList:
        cn = ""
        colTable = None
        colRName = None
        colAlias = None
        colSchema= None
        colName  = col

        sqlBruckets     = []
        sqlParentesist  = []

        if col.lower().find(" as") > 0:
            colAlias = col[col.lower().find(" as")+4:].strip()
            colName = col[:col.lower().find(" as ")].strip()
        for i in colName:
            if pre and pre==i:
                sqlBruckets.append(pre)
                cn+=i
            elif pos and pos==i:
                sqlBruckets.append(pos)
                cn+=i
            elif "("==i and len(sqlBruckets)==0:
                sqlParentesist.append ("(")
                cn += i
            elif ")"==i and len(sqlBruckets)==0:
                sqlParentesist.append (")")
                cn += i
            elif "." == i and len(sqlParentesist)==0:
                if colTable:
                    colSchema = colTable
                colTable = cn
                cn = ""
            elif " " == i and len(sqlBruckets)%2==0:
                t1 = 0
                t2 = 0
                for x in sqlParentesist:
                    if "(" == x: t1+=1
                    if ")" == x: t2+=1

                if t1==t2:
                    colRName=cn
                    cn = ""
            else:
                cn += i
        if not colRName:
            colRName = cn
        else:
            if cn and len(cn)>0:
                colAlias = cn

        retColList.append ({COLUMN_NAME:colRName, COLUMN_TABLE:colTable, COLUMN_ALIAS:colAlias, COLUMN_SCHEMA:colSchema})
    return retColList

def existsColumnInQuery (sqlStr, pre="[", pos="]"):
    ret = OrderedDict()
    existsColumns = extractSQLColumns (sqlStr, pre=pre, pos=pos)

    for col in existsColumns:
        colName     = col[COLUMN_NAME].replace(pre,"").replace(pos,"").lower()
        colValue    = col[COLUMN_NAME]
        if COLUMN_TABLE in col and col[COLUMN_TABLE] and len(col[COLUMN_TABLE])>0:
            colValue= '%s.%s' %(col[COLUMN_TABLE],colValue)
        if COLUMN_SCHEMA in col and col[COLUMN_SCHEMA] and len(col[COLUMN_SCHEMA])>0:
            colValue= '%s.%s' %(col[COLUMN_SCHEMA],colValue)
        ret[colName] = colValue
    return ret


##########  OLD FUNCTION #########################################3
## HELP FUNCTION : Return Dictioanry
#### {Columns: {src col:col name in target}}, Tables:{TableName:[All columns], '':[All column not identified in tables]}
def __extract_select_part (parsed):
    ret         = DIC.copy()
    columnList  = []
    columnDic   = DIC.copy()
    addToken    = False
    colNum      = 1

    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            addToken = True

        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            addToken = False
            break
        else:
            if addToken:
                dicKey  = None
                dicValue= None

                if isinstance(item, IdentifierList):
                    for identifier in item.get_identifiers():

                        identifier = str(identifier)
                        srcName = identifier

                        if identifier.lower().find(" as ") > 0:
                            srcName = identifier[:identifier.lower().find(" as ")].strip()
                            tarName = identifier[identifier.lower().find(" as ")+4:].strip()
                        else:
                            tarName = srcName.split(".")
                            tarName = tarName[1] if len(tarName)>1 else tarName[0]

                            columnList.append( (srcName.split(".") , tarName) )
                elif isinstance(item, Identifier):
                    item = str(item)
                    srcName = item
                    if item.lower().find(" as") > 0:
                        srcName = item[:item.lower().find(" as")].strip()
                        tarName = item[item.lower().find(" as")+3:].strip()
                    else:
                        tarName = srcName.split(".")
                        tarName = tarName[1] if len(tarName) > 1 else tarName[0]
                    columnList.append( (srcName.split(".") , tarName) )
                elif str(item) == '*':
                    columnList.append((['*'], '*'))


    for tupCol in columnList:

        col     = tupCol[0]
        tarName = tupCol[1]
        if col and len (col) == 1:
            dicKey = ''
            dicValue = col[0]
        elif col and len (col) >= 2:
            dicKey = col[0]
            dicValue = ".".join(col)
            dicValue = dicValue.replace("\n", " ")
        else:
            p("dbQueryParser->extract_select_part: ERROR Loading column identifier, will ignore column %s" % str(col), "i")
            continue

        dicValue = dicValue.split('.')
        dicValue = dicValue[1] if len(dicValue)==2 else dicValue[0]

        columnDic[COLUMNS][dicValue]=tarName

        if dicKey not in columnDic[TABLES]:
            columnDic[TABLES][dicKey] = []

        columnDic[TABLES][dicKey].append (dicValue)

    return columnDic


#sql = "select popay.* from popay"
#extract_TargetColumn (sql, pre="[", pos="]")