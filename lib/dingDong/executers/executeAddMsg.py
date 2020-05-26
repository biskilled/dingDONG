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
import os
import smtplib
from collections            import OrderedDict
from email.mime.multipart   import MIMEMultipart
from email.mime.text        import MIMEText

from dingDONG.config        import config
from dingDONG.misc.logger   import LOGGER_OBJECT,p
from dingDONG.executers.executeHTMLReport import eHtml, createHtmlFromList

class msgProp (object):
    STEP_NUM    = "NUM."
    DESC        = "DESCRIPTION"
    TS          = "TIME STAMP"
    STEP_TIME   = "EXEC TIME"
    TOTAL_TIME  = "AGG TIME"
    TASKS       = "CNT. TASKS"

    MSG_SUBJECT_SUCCESS = "LOADING JOB %s TOTAL TASKS EXEC: %s"
    MSG_SUBJECT_FAILURE = "ERROR LOADING JOB %s TOTAL TASKS EXEC: %s "
    MSG_LAST_STEP       = "TOTAL EXECUTION  "

    _TIME_FORMAT = "%m/%d/%Y %H:%M:%S"
    _PREFIX_DESC = "STATE_"

class executeAddMsg (object):

    def __init__ (self, timeFormat=msgProp._TIME_FORMAT, sDesc=msgProp._PREFIX_DESC ):
        self.startTime  = time.time()
        self.lastTime   = self.startTime
        self.stateDic   = OrderedDict()
        self.loggClass  = LOGGER_OBJECT
        self.timeFormat = timeFormat
        self.stateCnt   = 0
        self.sDesc      = sDesc
        self.inProcess  = False
        self.currentSateDic = None

        self.cntTasks   = 0

    def addState (self, sDesc=None, totalTasks=None):
        self.stateCnt+=1
        if not sDesc:
            sDesc="%s%s" %(str(self.sDesc),str(self.stateCnt))
        ts = time.time()
        tsStr = time.strftime(self.timeFormat, time.localtime(ts))
        tCntFromStart   = str(round ( ((ts - self.startTime) / 60) , 2))
        tCntFromLaststep= str(round ( ((ts - self.lastTime) / 60) , 2))
        self.lastTime   = ts
        totalTasks = 0 if not totalTasks or totalTasks<1 else totalTasks

        self.currentSateDic = OrderedDict()
        self.currentSateDic[msgProp.STEP_NUM]   = self.stateCnt
        self.currentSateDic[msgProp.DESC]       = sDesc
        self.currentSateDic[msgProp.TS]         = tsStr
        self.currentSateDic[msgProp.STEP_TIME]  = tCntFromLaststep
        self.currentSateDic[msgProp.TOTAL_TIME] = tCntFromStart
        self.currentSateDic[msgProp.TASKS]      = totalTasks

        if self.stateCnt>1:
            self.stateDic[self.stateCnt-1][msgProp.STEP_TIME]  = tCntFromLaststep
            self.stateDic[self.stateCnt - 1][msgProp.TOTAL_TIME] = tCntFromStart

            self.currentSateDic[msgProp.STEP_TIME] = 0
            self.currentSateDic[msgProp.TOTAL_TIME]= 0

        self.stateDic[self.stateCnt] = self.currentSateDic

    def addStateCnt (self):
        if self.currentSateDic and msgProp.TASKS in self.stateDic[self.stateCnt]:
            self.stateDic[self.stateCnt][msgProp.TASKS]+=1
        self.cntTasks+=1

    def end(self, msg=None,pr=True):
        msg = msg if msg else msgProp.MSG_LAST_STEP
        totalTasks = 0

        for col in self.stateDic:
            if self.stateDic[col][msgProp.TASKS] == 0:
                self.stateDic[col][msgProp.TASKS] = 1

            totalTasks += self.stateDic[col][msgProp.TASKS]

        self.addState(sDesc=msg, totalTasks=totalTasks)

        if pr:
            for col in self.stateDic:
                p (list(self.stateDic[col].values()))

    def sendSMTPmsg (self, msgName, onlyOnErr=False, withErr=True,withWarning=True ):

        okMsg = msgProp.MSG_SUBJECT_SUCCESS %(msgName, str(self.cntTasks))
        errMsg= msgProp.MSG_SUBJECT_FAILURE %(msgName, str(self.cntTasks))

        errList = self.loggClass.getLogData (error=True)
        errCnt  = len(errList) if errList else 0

        warList = self.loggClass.getLogData (error=False) if withWarning else None

        htmlList = []
        msgSubj  = okMsg if errCnt<1 else errMsg

        if onlyOnErr and errCnt>0 or not onlyOnErr:
            # First table - general knowledge
            self.addState(sDesc='')

            headerNames = False
            dicFirstTable = {eHtml.HEADER:[],eHtml.ROWS:[]}


            for st in self.stateDic:
                if not headerNames:
                    for k in self.stateDic[st]:
                        dicFirstTable[eHtml.HEADER].append (k)
                    headerNames = True

                dicFirstTable[eHtml.ROWS].append ( list(self.stateDic[st].values()) )

            htmlList.append (dicFirstTable)
            if withErr:
                if errList and len(errList)>0:
                    dicFirstTable = {eHtml.HEADER: ['ERROR FOUNDS IN CURRENT EXECUTION'],eHtml.ROWS: []}
                    for err in errList:
                        dicFirstTable[eHtml.ROWS].append ( [err] )
                else:
                    dicFirstTable = {eHtml.HEADER: ['NO ERROR FOUNDS IN CURRENT EXECUTION'], eHtml.ROWS: []}
                htmlList.append(dicFirstTable)

            if withWarning and warList and len (warList)>0:
                dicFirstTable = {eHtml.HEADER: ['WARNING FOUNDS IN CURRENT EXECUTION'],eHtml.ROWS: []}
                for war in warList:
                    dicFirstTable[eHtml.ROWS].append([war])

                htmlList.append(dicFirstTable)

            msgHtml = createHtmlFromList(htmlList=htmlList, htmlHeader=msgName)
            self.__sendSMTP(msgSubj=msgSubj, msgHtml=msgHtml)

    def __sendSMTP (self, msgSubj, msgHtml=None, msgText=None):
        sender          = config.SMTP_SENDER
        receivers       = ", ".join(config.SMTP_RECEIVERS)
        receiversList   = config.SMTP_RECEIVERS
        serverSMTP      = config.SMTP_SERVER
        serverUsr       = config.SMTP_SERVER_USER
        serverPass      = config.SMTP_SERVER_PASS

        msg = MIMEMultipart('alternative')
        msg['Subject']  = msgSubj
        msg['From']     = sender
        msg['To']       = receivers

        if msgText:
            textInMail = ''
            if isinstance(msgText, list):
                for l in msgText:
                    textInMail += l + "\n"
            else:
                textInMail = msgText

            msg.attach(MIMEText(textInMail, 'plain'))

        if msgHtml and len(msgHtml)>0:
            msg.attach( MIMEText(msgHtml, 'html') )

        try:
            server = smtplib.SMTP(serverSMTP)
            server.ehlo()

            if serverUsr and  serverPass:
                server.starttls()
                server.login(serverUsr, serverPass)

            server.sendmail(sender, receiversList, msg.as_string())
            server.quit()

        except smtplib.SMTPException:
            err = "gFunc->sendMsg: unable to send email to %s, subject is: %s " % (str(receivers), str(msgSubj))
            raise ValueError(err)