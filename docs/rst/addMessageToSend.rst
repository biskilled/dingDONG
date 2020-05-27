.. _tag_sendMsg:

Mailing logging massaging
=========================

`msg` method using to add masages for monitoring overall work-flow process

:addState: method to add massage into HTML e-mail
:sendSMTPmsg: send email massage with errors (if exists), warnings (if exists)


::

    """ import modules -> logging used fr setting log level"""
    from dingDong import DingDong
    from dingDong import Config

    Config.SMTP_RECEIVERS   = [<email1>, <email2>...]   # SEND EMAIL: TO
    Config.SMTP_SERVER      = "<SMTP server>"
    Config.SMTP_SERVER_USER = "email@address.com"
    Config.SMTP_SERVER_PASS = "<email password>"
    Config.SMTP_SENDER      = "<email from>"            # SEND EMAIL: FROM



    PROJECT_FOLDER      = <folder path>\JSON    # main folder to store all JSON work-flow files
    LOGS_FOLDER         = <folder path>\LOGS    # logs folder
    SQL_FOLDER          = <folder path>\SQL     # SQL files to execute


    FILES_NOT_INCLUDE   = ['<jsonFile.json>', '<jsonFile1>']    # JSON files to ignore while using JSON folder
    FILES_INCLUDE       = ['<jsonFile5.json>','<jsonFile8>']    # Load only this JSON files

    CONN_DICT = {
                'dwh' : {"conn":"sql" , "url":<URL string>,"file":"sample.sql"},
                'sap' : {"conn":"oracle", 'dsn':<dnn> , 'user':<user>,'pass':<pass>,'nls':<local nls language>},
                'crm' : {"conn":"sql" , "url":<URL string>},
                'file': {'delimiter':'~','header':True, 'folder':<folder path>,'replace':r'\"|\t'}
                }

    Config.QUERY_PARAMS = {
        "$start" : "1/1/2018"
        "$end"   : "/31/12/2019"
    }

    ddSQLExecution = [
            (1, SQL_FOLDER+"\\updateDWH.sql", {}),
            (2, "exec Procedure_1_SQL", {}),
            (3, "exec Procedure_2_SQL", {}),
            (3, "exec Procedure_3_SQL" , {}),
            (4, "exec Procedure_4_SQL", {}),
            (5, "exec Procedure_5_SQL @last_etl_date='$start'" ,{'$start':config.QUERY_PARAMS['$start']}),
            (5, "exec Procedure_6_SQL", {})
        ]

    if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Loading data from json files, cant get: source list files or destination list files or append mode () ')
        dd = dingDong(  dicObj=None, filePath=None, dirData=PROJECT_FOLDER,
                        includeFiles=FILES_INCLUDE, notIncludeFiles=FILES_NOT_INCLUDE,
                        dirLogs=LOGS_FOLDER, connDict=CONN_DICT, processes=4)

        dd.setLoggingLevel(val=logging.DEBUG)

        dd.ding()
        dd.msg.addState("DING FINISHED !")

        dd.dong()
        dd.msg.addState("DONG FINISHED !")

        dd.execDbSql(queries=qs, connName='sql')
        dd.msg.addState("FINISH BUSINESS LOGIC  !")

        dd.execMicrosoftOLAP(serverName='<OLAP_SERVER>', dbName='<OLAP_DB>', cubes=[], dims=[], fullProcess=True)
        dd.msg.addState("FINISH EXECUTING OLAP !")

        dd.msg.sendSMTPmsg (msgName="FINISHED WORK_FLOW", onlyOnErr=False, withErr=True, )