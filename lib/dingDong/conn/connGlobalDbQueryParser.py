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

import re
import sqlparse
from collections import OrderedDict
from sqlparse.sql       import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens    import Keyword, DML

from dingDong.misc.logger import p
from dingDong.misc.misc import replaceStr, uniocdeStr
from dingDong.config import config

QUERY_COLUMNS_KEY       = '~'
QUERY_NO_TABLE          = '~notFound~'
QUERY_PRE               = '~pre~'
QUERY_POST              = '~post~'

TABLE_ALIAS = 'alias'
TABLE_SCHEMA= 'schema'
TABLE_NAME  = 'tbl'
TABLE_COLUMN= 'column'

## extract_tableAndColumns -->
##      Using extract_tables which return Dictionary : {'Columns': {ColName:ColTargetName....}, 'Tables':{tableName: [Columns ....]}}
##      Using extract_table_identifiers which return list of [(table aliace, table schema, table Name) ...]
def extract_tableAndColumns (sql):
    sql, pre = removeProps (sql=sql)
    tblTupe , columns, sqlPre, sqlPost = extract_tables(sql)
    pre = pre if pre and len(pre)>0 else sqlPre

    ret = {QUERY_PRE:pre, QUERY_POST:sqlPost}

    # merge table to columns

    tableAliasVsName = {}
    foundTables = []
    for tbl in tblTupe:
        alias       = tbl[0]
        schamenName = tbl[1]
        tableName   = tbl[2]

        aliasTbl = alias if alias is not None and len(alias)>0 else tableName
        tableAliasVsName[aliasTbl] = tableName

        if tableName not in ret:
            ret[tableName] = {TABLE_ALIAS:alias, TABLE_SCHEMA:None if schamenName==tableName else schamenName}

        if columns and len (columns)>0:
            ret[tableName][TABLE_COLUMN] = []

            for col in columns:
                if str(col) in [tableName,alias]:
                    ret[tableName][TABLE_COLUMN].extend  (columns[col])
                    foundTables.append (col)


    for tbl in columns:
        if tbl not in foundTables:
            if QUERY_NO_TABLE not in ret:
                ret[QUERY_NO_TABLE] = {TABLE_ALIAS:None, TABLE_SCHEMA:None,TABLE_COLUMN:[]}
            ret[QUERY_NO_TABLE][TABLE_COLUMN].extend ( columns[tbl] )
    return ret

def removeProps (sql):
    sql = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql)
    sql = re.sub(r"\s+", " ", sql)
    pre = ""

    regSql = r"(.*?select\s+(?:top\s+\d+\s+){0,1}(?:distinct\s+){0,1})(.*)"

    # sqlGrouping = re.search(regexSql, sqlStr, re.IGNORECASE | re.MULTILINE)
    sqlGrouping = re.search(regSql, sql, re.UNICODE | re.MULTILINE | re.S | re.I)
    if sqlGrouping:
        pre = sqlGrouping.group(1)
        sql = '%s %s'  %('SELECT', sqlGrouping.group(2))
    return sql, pre


def extract_tables(sql):
    # let's handle multiple statements in one sql string
    extracted_tables = []
    extracted_last_tables_Tuple = None
    extracted_last_columns_Dic = None
    preSql = ""
    postSql= ""
    # replacements to SQL queries
    sql = replaceStr (sString=sql,findStr="ISNULL (", repStr="ISNULL(", ignoreCase=True,addQuotes=None)
    sql = replaceStr(sString=sql, findStr="CONVERT (",repStr="CONVERT(", ignoreCase=True, addQuotes=None)
    sql = sql.replace("\t"," ")
    statements = list(sqlparse.parse(sql))

    for statement in statements:
        if statement.get_type() != 'UNKNOWN':
            stream = extract_from_part(statement)
            # will get only last table column definistion !
            extracted_last_columns_Dic,preSql,postSql  = extract_select_part(statement)
            extracted_last_tables_Tuple = list (extract_table_identifiers(stream))
            #extracted_tables.append(list(extract_table_identifiers(stream)))
    #return list(itertools.chain(*extracted_tables))
    return (extracted_last_tables_Tuple , extracted_last_columns_Dic,preSql,postSql)

def extract_from_part(parsed):
    from_seen = False
    for item in parsed.tokens:
        if item.is_group:
            for x in extract_from_part(item):
                yield x
        if from_seen:
            if is_subselect(item):
                for x in extract_from_part(item):
                    yield x
            elif item.ttype is Keyword and item.value.upper() in ['ORDER', 'GROUP', 'BY', 'HAVING']:
                from_seen = False
                StopIteration
            else:
                yield item
        if item.ttype is Keyword and item.value.upper() == 'FROM':
            from_seen = True

def is_subselect(parsed):
    if not parsed.is_group:
        return False
    for item in parsed.tokens:
        if item.ttype is DML and item.value.upper() == 'SELECT':
            return True
    return False

def extract_select_part (parsed):
    nonColumnSign       = QUERY_NO_TABLE
    columnList          = []
    columnDic           = {}
    col                 = None
    addToken            = False
    preSql              = ""
    postSql             = ""
    isPost              = False

    for item in parsed.tokens:
        if not addToken and not isPost:
            preSql+=uniocdeStr(item.value)
        if item.ttype is DML and item.value.upper() == 'SELECT':
            addToken = True

        elif item.ttype is Keyword and item.value.upper() == 'FROM':
            addToken = False
            isPost   = True
            postSql += uniocdeStr(item.value)

        elif addToken:
            dicKey  = None
            dicValue= None

            if isinstance(item, IdentifierList):
                for identifier in item.get_identifiers():
                    identifier = str(identifier)
                    srcName = identifier

                    if identifier.lower().rfind(" as ") > 0:
                        srcName = identifier[:identifier.lower().rfind(" as ")].strip()
                        tarName = identifier[identifier.lower().rfind(" as ")+4:].strip()
                    else:
                        tarName = srcName.split(".")
                        tarName = tarName[1] if len(tarName)>1 else tarName[0]

                    columnList.append( (srcName.replace("\n", " ").split(".") , tarName) )
            elif isinstance(item, Identifier):
                item = str(item)
                srcName = item
                if item.lower().find(" as") > 0:
                    srcName = item[:item.lower().find(" as")].strip()
                    tarName = item[item.lower().find(" as")+3:].strip()
                else:
                    tarName = srcName
                columnList.append( (srcName.replace("\n", " ").split(".") , tarName) )
            elif not isPost:
                preSql += uniocdeStr(item.value)
        elif isPost:
            postSql +=uniocdeStr(item.value)

    for tupCol in columnList:
        col     = tupCol[0]
        tarName = tupCol[1]
        if col and len (col) == 1:
            dicKey = nonColumnSign
            dicValue = col[0]
        elif col and len (col) >= 2:
            dicKey = col[0]
            dicValue = col
            if dicKey not in columnDic:
                columnDic[dicKey] = []
        else:
            p("ERROR Loading column identifier, will ignore column %s" % str(col), "i")
            continue

        if dicKey and dicKey not in columnDic:
                columnDic[dicKey] = []
        dicValue = tuple(dicValue) if isinstance(dicValue,list) else dicValue
        dicValue = (dicValue,) if isinstance(dicValue,basestring) else dicValue
        columnDic[dicKey].append ((dicValue,tarName))

    return columnDic, preSql, postSql

def extract_table_identifiers(token_stream):
    for item in token_stream:
        if isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                value = ( identifier.get_alias() , identifier._get_first_name(), identifier.get_real_name())
                value = [x.replace('"', '').replace("'", "") if x else None for x in  value]
                value = tuple( value )
                yield value

        elif isinstance(item, Identifier):
            value = (item.get_alias(), item._get_first_name(), item.get_real_name())
            value = [x.replace('"', '').replace("'", "") if x else None for x in value]
            value = tuple(value)
            yield value

""" PARSE QUERY --> GET QUERIES BY USING REGEX """
def querySqriptParserIntoList(sqlScript, getPython=True, removeContent=True, dicProp=None):
    if isinstance(sqlScript, (tuple, list)):
        sqlScript = "".join(sqlScript)
    # return list of sql (splitted by list of params)
    allQueries = __getAllQuery(longStr=sqlScript)
    if getPython:
        allQueries = __getPythonParam(allQueries, mWorld=config.PARSER_SQL_MAIN_KEY)

    if removeContent:
        allQueries = __removeComments(allQueries)

    if dicProp:
        allQueries = __replaceProp(allQueries, dicProp)

    return allQueries

def __getAllQuery(longStr, splitParam=['GO', u';']):
    sqlList = []
    for splP in splitParam:
        if len(sqlList) == 0:
            sqlList = longStr.split(splP)
        else:
            tmpList = list([])
            for sql in sqlList:
                tmpList.extend(sql.split(splP))
            sqlList = tmpList
    return sqlList

def __getPythonParam(queryList, mWorld):
    ret = []
    for query in queryList:
        # Delete all rows which are not relevant
        # Regex : <!popEtl XXXX/>
        # fPythonNot = re.search(r"<!%s([^>].*)/>" % (mWorld), query,flags=re.IGNORECASE | re.MULTILINE | re.UNICODE | re.DOTALL | re.S)
        # Regex : <!popEtl> ......... </!popEtl>
        reg = re.finditer(r"<!%s(.+?)</!%s>" % (mWorld, mWorld), query,
                          flags=re.IGNORECASE | re.MULTILINE | re.UNICODE | re.DOTALL | re.S)
        if reg:
            for regRemove in reg:
                query = query.replace(regRemove.group(0), "")

        # Add python queries into return list
        # Regex : <popEtl STRING_NAME> ....... </popEtl>
        # fPython2    = re.search(r"<%s.*/%s>" % (mWorld,mWorld),   query, flags = re.IGNORECASE | re.MULTILINE | re.UNICODE | re.DOTALL | re.S)

        # Regex : <popEtl STRING_NAME>......</popEtl> --> Take string to the end
        reg = re.finditer(r"<%s(.+?)>(.+?)</%s>" % (mWorld, mWorld), query,flags=re.IGNORECASE | re.MULTILINE | re.UNICODE | re.DOTALL | re.S)
        if reg:

            for i, regFind in enumerate(reg):
                pythonSeq = regFind.group(0)
                pythonVar = regFind.group(1).strip()
                querySql = regFind.group(2).strip()

                if i == 0 and regFind.start() > 0:
                    queryStart = query[: query.find(pythonSeq)].strip()
                    if queryStart and len(queryStart) > 0:
                        ret.append((None, queryStart))

                ret.append((pythonVar, querySql))
        else:
            if query and len(query.strip()) > 0:
                ret.append((None, query.strip()))
    return ret

def __removeComments(listQuery, endOfLine='\n'):
    retList = []
    for s in listQuery:
        isTup = False
        if isinstance(s, (tuple, list)):
            pre = s[0].strip() if s[0] else None
            post = s[1].strip()
            isTup = True
        else:
            post = s.strip()

        post = re.sub(r"--.*$", r"", post, flags=re.IGNORECASE | re.MULTILINE | re.UNICODE).replace("--", "")
        post = re.sub(r'\/\*.*\*\/', "", post, flags=re.IGNORECASE | re.MULTILINE | re.UNICODE | re.DOTALL)
        post = re.sub(r"print .*$", r"", post, flags=re.IGNORECASE | re.MULTILINE | re.UNICODE).replace("print ",
                                                                                                        "")

        if endOfLine:
            while len(post) > 1 and post[0:1] == "\n":
                post = post[1:]

            while len(post) > 1 and post[-1:] == "\n":
                post = post[:-1]

        if not post or len(post) == 0:
            continue
        else:
            if isTup:
                retList.append((pre, post,))
            else:
                retList.append(post)

    return retList

def __replaceProp(allQueries, dicProp):
    ret = []
    for query in allQueries:
        if isinstance(query, (list, tuple)):
            pr1 = query[0]
            pr2 = query[1]
        else:
            pr2 = query
        if not pr1 or pr1 and pr1 != "~":
            for prop in dicProp:
                pr2 = (replaceStr(sString=pr2, findStr=prop, repStr=dicProp[prop], ignoreCase=True))

        tupRet = (pr1, pr2,) if isinstance(query, (list, tuple)) else pr2
        ret.append(tupRet)
    return ret

sql = """SELECT Top 1000 t.*, t.CLASS AS INTSugMev, t.CLASS AS KUKU,t.CLASS,
t.CKTXT AS UNSugMevDescr,CASE t.CLASS WHEN '0002' THEN 'BBB' ELSE t.CKTXT END AS Test,
CAST(t.CKTXT AS varchar(100)) As test2,
(SELECT SYSDATE FROM dual)  As gggg
FROM SAPR3.TNP01T t, SAPR3.TNP01 p where t.SPRAS = 'B' and t.CLASS = p.CLASS"""
#xx = extract_tableAndColumns (sql)

#print ("TAL 1234")
#for x in xx:
#    print (x, xx[x])