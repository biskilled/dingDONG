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
import re
import io
import sys
import multiprocessing
from collections import OrderedDict

from dingDong.config        import config
from dingDong.misc.logger   import p

if 2 == sys.version_info[0]:
    reload(sys)
    sys.setdefaultencoding(config.FILE_ENCODING)

# replace paramters will change from sp regex expression
# sql server match = @[Paramter] = values
# sql server replace =;@\s to ''
def __replaceParameters (connType, line, dicParam):
    delimiterChar = ';'
    arrLine = line.split(delimiterChar)
    arrRet = []

    for tup in arrLine:
        fmatch = re.search(config.DATA_TYPE['sp'][connType]['match'], line)
        if fmatch:
            varKey  = re.sub (config.DATA_TYPE['sp'][connType]['replace'],'',fmatch.group(1))
            varValue= re.sub (config.DATA_TYPE['sp'][connType]['replace'],'',fmatch.group(2))

            if varKey in dicParam:
                tup = tup.replace( varValue , dicParam[varKey] )
        arrRet.append(tup)

    return delimiterChar.join (arrRet)

def __split_sql_removeString (line, remove=False):

    #if remove:
    #    if "*/" in line:
    #        line = re.sub(r'.*\*\/', '', line)
    #        remove = False
    #    else:
    #        return '', True
    line = line.strip()
    line = re.sub(r'\/\*.*\*\/', '', line)
    line = re.sub(r'--.*', '', line)


    #if "/*" in line:
    #    remove = True
    #    line = re.sub(r'\/\*.*', '', line)
    line = line.replace("\n", " ").replace("\t", " ")
    return line, remove

def __split_sql_expressions(text):
    if (isinstance(text, str)):
        return [text]
    results = []
    tup = ''
    remove = False
    for line in text:
        if u";" in line:
            lines = line.split (";")
            for l in lines:
                tup+=" "+l
                tup , remove = __split_sql_removeString (tup, remove)
                if len (tup) > 0:
                    results.append (tup)
                tup = ''
        else:
            tup+=" "+line
            tup, remove = __split_sql_removeString(tup, remove)
    if len (tup) > 0:
        results.append(tup)
    return results

def __execSql ( params ):
    (sqlScript, locParams, connObj) = params
    connObj.connect()
    def __execEachLine (connObj, sqlTxt):
        sqlQuery = __split_sql_expressions(sqlTxt)

        isParam = True if len(locParams) > 0 else False
        for line in sqlQuery:
            if isParam:
                line = __replaceParameters(connObj.cType, line, locParams)
            if 'PRINT' in line:
                disp = line.split("'")[1]
                p('SQL PRINT: ' + disp, "i")
            if len(line) > 1:
                connObj.execSP(line)
                p ("loadExecSP->__execSql Finish Executing : %s " %line, "i")

    if str(sqlScript).endswith(".sql") and os.path.isfile(sqlScript):
        with io.open(sqlScript, 'r',  encoding=config.FILE_ENCODING) as inp:
            __execEachLine(connObj, inp)

    else:
        __execEachLine(connObj, sqlScript)
    connObj.close()


def __execParallel (priority, ListOftupleFiles, connObj):
    multiProcessParam = []
    multiProcessFiles = ''

    for tupleFiles in ListOftupleFiles:
        sqlFiles    = tupleFiles[0]
        locParams   = tupleFiles[1]

        for sqlScript in sqlFiles:
            multiProcessParam.append( (sqlScript, locParams, connObj,) )
            multiProcessFiles += "'" + sqlScript + "' ; "

    # single process
    if priority<0 or len(multiProcessParam)<2:
        p("loadExcelSP->__execParallel: SINGLE process: %s" % (str(multiProcessFiles)), "ii")
        for query in multiProcessParam:
            __execSql(query)

    # multiprocess execution
    else:
        if len(multiProcessParam) > 1:
            p ("loadExcelSP->__execParallel: MULTI process: %s" %(str(multiProcessFiles)), "ii")
            # Strat runing all processes
            proc = multiprocessing.Pool(config.NUM_OF_PROCESSES).map( __execSql ,multiProcessParam )

    p("loadExcelSP->__execParallel: FINISH Excecuting priority %s, loaded files: %s >>>> " %(str(priority), str (multiProcessFiles)), "i")

# sqlWithParamList --> list of tuple (file name, paramas)
def execQuery (sqlWithParamList, connObj ):
    if sqlWithParamList is None or len(sqlWithParamList)==0:
        p("NOT RECIAVE ANY SQL STATEMENT")
        return

    if isinstance(sqlWithParamList, str):
        sqlWithParamList = [sqlWithParamList]

    connObj.connect()
    allFiles    = {}
    sqlFiles    = []
    locParams   = {}

    for script in sqlWithParamList:
        parallelProcess = -1
        if len (script) == 3:
            parallelProcess = script[0]
            locName         = script[1]
            locParams       = script[2]
        elif len (script) == 2:
            locName         = script[0]
            locParams       = script[1]
        elif len (script) == 1:
            locName         = script[0]
        else:
            p("NOT CONFIGURE PROPERLY, MUST HAVE 1, 2 or 3 in a tuple  %s " % (str(script)) ,"e")
            break

        # sql file is list of all files to execute
        if os.path.isdir(locName):
            p("CONNENCTION:%s, DIRECTORY: %s " % (connObj.conn, str(locName)), "ii")
            sqlFiles = [os.path.join(locName, pos_sql)  for pos_sql in os.listdir(locName) if pos_sql.endswith('.sql')]
        elif os.path.isfile(locName):
            p("CONNENCTION:%s, PARALLEL:%s, FILE: %s  " % (connObj.conn, str(parallelProcess), str(locName)), "ii")
            sqlFiles.append(locName)
        else:
            p("CONNENCTION:%s, QUERY: %s" % (connObj.conn, str(locName) ), "ii")
            sqlFiles.append(locName)

        # Adding all script into ordered dictionary

        if parallelProcess not in allFiles:
            allFiles[parallelProcess] = []

        allFiles[parallelProcess].append ( (sqlFiles, locParams) )
        sqlFiles = list([])

    for priority  in OrderedDict (sorted (allFiles.items())):

        p ('Executing prioiriy %s >>>>' %str(priority), "ii")
        __execParallel (priority, allFiles[priority], connObj)

    connObj.close()