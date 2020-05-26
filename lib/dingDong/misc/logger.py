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

import time
import logging
import inspect
import os

from dingDONG.config import config


### INTERNAL LOGGING CLASSES


class __myLogger (object):
    class __logFilter(object):
        def __init__(self, level):
            self.__level = level

        def filter(self, logRecord):
            return logRecord.levelno <= self.__level

    def __init__ (self, loggLevel=logging.DEBUG, logFormat='%(asctime)s %(levelname)s %(message)s' ):
        dateFormat          = '%Y-%m-%d %H:%M:%S'
        self.logFormatter   = logging.Formatter(logFormat, dateFormat)
        self.logLevel       = loggLevel

        logging.basicConfig(level=self.logLevel, format=logFormat, datefmt=dateFormat)
        self.isLogsFilesInit= False
        self.logTmpFileErr  = None
        self.logTmpFileWar  = None
        self.logDir         = config.LOGS_DIR
        self.logg =  logging.getLogger(__name__)

        if config.LOGS_DIR and os.path.isdir(config.LOGS_DIR):
            self.setLogsFiles(logDir=config.LOGS_DIR)

        #if logStdout:
        #    consoleHandler = logging.StreamHandler(sys.stdout)
        #    consoleHandler.setFormatter(self.logFormatter)
        #    self.logg.addHandler(consoleHandler)

    def setLogsFiles (self, logDir=None, timeFormat='%Y%m%d'):
        self.logDir = logDir if logDir else config.LOGS_DIR

        if not self.logDir or len(self.logDir)<1:
            p("DIRECTORY FOR LOGS IS NOT DEFINED ")
            return

        if not os.path.isdir(self.logDir):
            err = "LOGS DIRECTORY: %s IS NOT VALID !" %(self.logDir)
            raise ValueError(err)

        currentDate = time.strftime(timeFormat)
        logFile = config.LOGS_INFO_NAME
        logFile = "%s_%s.log"%(logFile, currentDate) if logFile and ".log" not in logFile.lower() else logFile

        logErrFile = config.LOGS_ERR_NAME
        logErrFile = "%s_%s.err" % (logErrFile, currentDate) if logErrFile and ".err" not in logErrFile.lower() else logErrFile

        logTmpFile = config.LOGS_TMP_NAME
        logTmpFileErr = "%s.err" % (logTmpFile) if logTmpFile and ".err" not in logTmpFile.lower() else logTmpFile
        logTmpFileWar = "%s.warning" % (logTmpFile) if logTmpFile and ".warning" not in logTmpFile.lower() else logTmpFile

        if not os.path.isdir(self.logDir):
            err = "%s if not a correct directory " % self.logDir
            raise ValueError(err)

        if logTmpFile:
            ### Error Temp log file
            self.logTmpFileErr = os.path.join(self.logDir, logTmpFileErr)
            tmpFileErrors   = logging.FileHandler(self.logTmpFileErr, mode='a')
            tmpFileErrors.setFormatter(self.logFormatter)
            tmpFileErrors.setLevel(logging.ERROR)
            tmpFileErrors.addFilter(self.__logFilter(logging.ERROR))
            self.logg.addHandler(tmpFileErrors)

            self.logTmpFileWar = os.path.join(self.logDir, logTmpFileWar)
            tmpFileWarning = logging.FileHandler(self.logTmpFileWar, mode='a')
            tmpFileWarning.setFormatter(self.logFormatter)
            tmpFileWarning.setLevel(logging.WARNING)
            tmpFileWarning.addFilter(self.__logFilter(logging.WARNING))
            self.logg.addHandler(tmpFileWarning)

            try:
                if os.path.isfile(self.logTmpFileErr):
                    open(self.logTmpFileErr, 'w').close()

                if os.path.isfile(self.logTmpFileWar):
                    open(self.logTmpFileWar, 'w').close()
            except:
                p("CANNOT DELETE TEMP FILES\n     %s\n     %s" %(self.logTmpFileErr, self.logTmpFileWar), "e")

        if not logErrFile:
            fileHandler = logging.FileHandler(os.path.join(self.logDir, logFile), mode='a')
            fileHandler.setFormatter(self.logFormatter)
            self.logg.addHandler(fileHandler)
        else:
            # log file info
            fileHandlerInfo = logging.FileHandler(os.path.join(self.logDir, logFile))
            fileHandlerInfo.setFormatter(self.logFormatter)
            fileHandlerInfo.setLevel(self.logLevel)
            self.logg.addHandler(fileHandlerInfo)

            # Err file info
            fileHandlerErr = logging.FileHandler(os.path.join(self.logDir, logErrFile), mode='a')
            fileHandlerErr.setFormatter(self.logFormatter)
            fileHandlerErr.setLevel(logging.ERROR)
            self.logg.addHandler(fileHandlerErr)

        ## Delete OLD log file
        self.deleteLogFiles()

    def getLogg (self):
        return self.logg

    def getLogsDir (self):
        return self.logDir

    def setLogLevel (self, logLevel):
        self.logLevel = logLevel
        self.logg.setLevel(self.logLevel)

    def getLogData (self, logPath=None, error=True):
        def getLines (dir=None, f=None, fPath=None):
            lines = None
            logPath = os.path.join(dir, f) if dir and f else fPath

            if logPath and os.path.isfile(logPath):
                with open(logPath) as f:
                    lines = f.read().splitlines()
            return lines

        if logPath:
            lines = getLines (dir=None, f=None, fPath=logPath)
        elif error:
            lines = getLines(dir=self.logDir, f=self.logTmpFileErr, fPath=None)
        else:
            lines = getLines(dir=self.logDir, f=self.logTmpFileWar, fPath=None)

        return lines

    def deleteLogFiles (self, days=None ):
        days = days if days else config.LOGS_HISTORY_DAYS

        if self.logDir:
            now = time.time()
            old = now - (days * 24 * 60 * 60)
            for f in os.listdir(self.logDir):
                path = os.path.join(self.logDir, f)
                if os.path.isfile(path):
                    stat = os.stat(path)
                    if stat.st_mtime < old:
                        self.logg.info("DELETE FILE %s" %(path))
                        os.remove(path)

class __listHandler(logging.Handler):  # Inherit from logging.Handler
    def __init__(self):
        logging.Handler.__init__(self)
        # Our custom argument
        self.log_list = []

    def emit(self, record):
        # record.message is the log message
        self.log_list.append(self.format(record))

    def getList (self):
        return self.log_list


LOGGER_OBJECT  = __myLogger(loggLevel=config.LOGS_DEBUG)
__logg  = LOGGER_OBJECT.getLogg()

def p(msg, ind='I'):
    func = inspect.currentframe().f_back.f_code
    fileName = os.path.split(func.co_filename)[1].replace(".py", "")

    ind = ind.upper()
    indPrint = {'E': 'ERROR>> ',
                'I': 'INFO >> ',
                'W': 'WARNING >>',
                'II': 'DEBUG>> ',
                'III': 'Progress>> '}

    if 'III' in ind:
        __logg.debug("\r %s %s" %(indPrint[ind], msg))
    elif 'II' in ind:
        __logg.debug("%s->%s: %s" %(fileName, func.co_name, msg))
    elif 'I' in ind:
        __logg.info("%s->%s: %s" %(fileName, func.co_name, msg))
    elif 'W' in ind:
        __logg.warning("%s->%s: %s" % (fileName, func.co_name, msg))
    else:
        __logg.error("%s,%s->%s : %s " % (fileName, func.co_firstlineno, func.co_name,msg))