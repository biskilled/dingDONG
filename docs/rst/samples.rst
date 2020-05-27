.. _tag_samples:

Global configuration
====================

Global configuration can be stored in config file or in each work-flow.
Usually used for SMTP massaging, logging level and for folder locations

Global varaible will be used at dingDong init

::

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


Ding-Dong (mapp-load)
=====================

Sample of extracting 3 CSV files into temporal SqlLite tables. Creating a query to store aggragated data into
results table, and extracting all results into CSV file.

::

    """ import modules -> logging used fr setting log level"""
    import logging
    from dingDong import DingDong
    from dingDong import Config

    """ set log level: logging.INFO, logging.DEBUG, logging.ERROR """
    Config.LOGS_DEBUG = logging.DEBUG

    """ Config all connection URl
        Can be used by update Config.CONN_URL property or by send dictionary into connDict property at DingDong class init`
        key : can be general connection name , or connection type (sql, oracle, file .. )
        value:
            String--> connection string URL (key will be used to defined connection type: sql, oracle, mySql....
            Dictionary -->
                'conn' -> connenction type. full type list can be found at dingDong.misc.enumsJson.eConn static class
                'url'  -> connection URL
    """
    Config.CONN_URL = {
        'sampleSql': {'conn': 'sql',"url": "<Sql server connection string>;UID=USER;PWD=PWD;"},
        'file': "C:\\dingDong\\",
        'sqlite': "C:\\dingDong\\sqlLiteDB.db"}

    """ This is sample JSON configuration format for:
        1. mapping and loading CSV file named DATAELEMENTDESCRIPTION into SQLLite table named dateElements_Desc
        2. mapping and loading CSV file named DEMOGRAPHICS into SQLLite table named demographics
        3. mapping and loading CSV file named MEASURESOFBIRTHANDDEATH into SQLLite table named birthDate
        4. create a new query based on demographics and birthDate  into new table named `Final`
        5. Update sample field at `Final` table by using direct PL/SQL query
        6. Extract Final table data into a CSV file

        file default datatype can be found at dingDong.conn.baseBatch under DEFAULTS values (currently set to VARCHAR(200) for all relation Dbs
    """
    nodesToLoad = [
        {"source": ["file", "DATAELEMENTDESCRIPTION.csv"],
         "target": ["sqlite", "dateElements_Desc"]},

        {"source": ["file", "DEMOGRAPHICS.csv"],
         "target": ["sqlite", "demographics"]},

        {"source": ["file", "MEASURESOFBIRTHANDDEATH.csv"],
         "target": ["sqlite", "birthDate"]},

        {"query": ["sqlite", """   Select d.[State_FIPS_Code] AS A, d.[County_FIPS_Code] AS B, d.[County_FIPS_Code] AS G,d.[County_FIPS_Code], d.[CHSI_County_Name], d.[CHSI_State_Name],[Population_Size],[Total_Births],[Total_Deaths]
                                        From demographics d INNER JOIN birthDate b ON d.[County_FIPS_Code] = b.[County_FIPS_Code] AND d.[State_FIPS_Code] = b.[State_FIPS_Code]"""],
         "target": ["sqlite", "Final", 2]},

        {"myexec": ["sqlite", "Update dateElements_Desc Set [Data_Type] = 'dingDong';"]},

        {"source": ["sqlite", "Final"],
         "target": ["file", "finall.csv"]}
    ]

    """
        Init class DingDong"
            dicObj -> loading node mapping dictionay (as the listed sample)
            dirData-> will load all JSON configuration file located at this folder
            includeFiles    -> FILTER to load list of files in dirData folder
            notIncldeFiles  -> FILTER to remove list of files in dirData folder
            connDixt -> update all connection url. same property as Config.CONN_URL
            processes -> number of parrallel processing for loading data (DONG module)
    """

    dd = DingDong(dicObj=nodesToLoad, filePath=None, dirData=None,
                 includeFiles=None,notIncludeFiles=None,connDict=None, processes=1)

    dd.msg.addState("Start Ding")

    """ Mapping files structure into a table structure
        Target not exists   -> create new target table based on source table definitions
        Target exists       -> if there is change, there are 3 option to update the target table structure
            1. copy old data into the table with date prefix and create a new table with updated metadata (default, CODE:-1)
            2. create new table schema, store old schema in a copied table with date prefix and merge data from the old structure into a new structure (CODE: 1, updated at target or merge key values)
            3. no change can be made into this table. CODE number 2. can be added only to target or merge objects
    """
    dd.ding()

    """ Extracting and loading data from source to target or to merge
        if STT node exists in JSON mapping -> will update fields accordingly
        if the column node exists -> will map column types by column node definition
        if mapping node exists-> will map source to target accordingly

        more detild can be found at decumentation
    """
    dd.msg.addState("Start Dong")
    dd.dong()

    dd.msg.end(msg="FINISHED",pr=True)


PL\Sql Executor
===============

dingDong using execution methods to allow managing all business logic workflows
the simple below using a private function to set query parameters.
execution is done in parallel by define priorities. in our sample all priority number 1
will execute in parallel, same for priority 2 and so on.
Each execution can reciave paramters as a dcitioanry.
each step is moitored by the logging mechanism **dd.msg.addState("step desc")** is used for adding massages
and **dd.msg.sendSMTPmsg** send an HTML massage using SMTP confguration.

::

    # sample of private function to manage start date and end date parameters for SQL queries
    # current sample - receive days and return startDate and endDate in %Y%m%d format

    def setStartEndTime (e=1, s=400, f="%Y%m%d"):
        dataRange, curDate = (e,s,f,) , datetime.datetime.today()
        startDay = (curDate - datetime.timedelta(days=dataRange[1])).strftime(dataRange[2])
        endDay   = (curDate - datetime.timedelta(days=dataRange[0])).strftime(dataRange[2])
        return startDay, endDay

    # update SQL queries parameters

    startDay, endDay =  setStartEndTime (e=1, s=1000, f="%Y%m%d")
    config.QUERY_PARAMS = {
        "$start" : startDay,
        "$end"   : endDay
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

   dd = dingDong(  dicObj=None, filePath=None, dirData=PROJECT_FOLDER,
                    includeFiles=FILES_INCLUDE, notIncludeFiles=FILES_NOT_INCLUDE,
                    dirLogs=LOGS_FOLDER, connDict=CONN_DICT, processes=4)

    dd.setLoggingLevel(val=logging.DEBUG)
    dd.execDbSql(queries=qs, connName='sql')
    dd.msg.addState("FINISH ALL SQL QUERIES !")

    dd.msg.sendSMTPmsg (msgName="FINISHED EXECUTING WORK-FLOW", onlyOnErr=False, withErr=True, )



Source to target mapping (STT)
==============================

::

    #################################################
    #########       SAMPLE JSON FILE        #########
    #################################################
    [
       {
        "target": ["sql", "STG_Services"],
        "query": ["oracle", [
                    "SELECT COL1 as col1_Desc , COL2 as col2_Desc, COL3 as ValidEndDate, COL4 as ValidBgDate , COL5 as col5_Desc,",
                    "COL6 as col6_Desc, COL7 as col7_Desc, COL8 as col8_Desc, COL9 as col8_Desc ",
                    "FROM sar.services where COL7 ='B'"]
                    ],
        "exec":["sql", "update_Target_STG_Services.sql"],
        "merge":["DWH_Services",["COL1","COL2"]],
        "sttappend":{
            "ValidEndDate":{"s":"COL3", "t":"smalldatetime", "f":"fDCast()"},
            "ValidBgDate": {"s":"COL4", "t":"smalldatetime", "f":"fDCast()"},
            "LongDesc"   : {"t":"nvarchar(500)","e":"{COL6}{COL7}{COL8}"},
            "ETL_Date":    {"t":"smalldatetime","f":"fDCurr()"}
        },
        "index":[{"c":["COL1", "COL2"],"ic":true,"iu":False}]
       }
    ]

    #################################################
    #########       SAMPLE PYTHON FILE      #########
    #################################################

    # Global configuration

    from dingDong.config import config
    from dingDong.bl.ddExecuter import dingDong

    config.SMTP_RECEIVERS   = [<email1>, <email2>...]   # SEND EMAIL: TO
    config.SMTP_SERVER      = "<SMTP server>"
    config.SMTP_SERVER_USER = "email@address.com"
    config.SMTP_SERVER_PASS = "<email password>"
    config.SMTP_SENDER      = "<email from>"            # SEND EMAIL: FROM

    # Init folder paths
    PROJECT_FOLDER      = <folder path>\JSON    # main folder to store all JSON work-flow files
    LOGS_FOLDER         = <folder path>\LOGS    # logs folder
    SQL_FOLDER          = <folder path>\SQL     # SQL files to execute

    FILES_NOT_INCLUDE   = []    # JSON files to ignore while using JSON folder
    FILES_INCLUDE       = []    # Load only this JSON files

    # Init connection properties
    CONN_DICT = {
              'dwh' : {"conn":"sql" , "url":<URL string>,"file":"sample.sql"},
              'sap' : {"conn":"oracle", 'dsn':<dnn> , 'user':<user>,'pass':<pass>,'nls':<local nls language>},
              'crm' : {"conn":"sql" , "url":<URL string>},
              'file': {'delimiter':'~','header':True, 'folder':<folder path>,'replace':r'\"|\t'}
              }

    # list for PL/SQL execution script
    ddSQLExecution = [
        (1, SQL_FOLDER+"\\updateDWH.sql", {}),
        (2, "exec Procedure_1_SQL", {}),
        (3, "exec Procedure_2_SQL", {}),
        (3, "exec Procedure_3_SQL" , {}),
        (4, "exec Procedure_4_SQL", {}),
        (5, "exec Procedure_5_SQL @last_etl_date='$start'" ,{'$start':config.QUERY_PARAMS['$start']}),
        (5, "exec Procedure_6_SQL", {})
    ]

    # private function for managing paramteres
    def _setStartEndTime (e=1, s=100, f="%Y%m%d"):
        dataRange, curDate = (e,s,f,) , datetime.datetime.today()
        startDay = (curDate - datetime.timedelta(days=dataRange[1])).strftime(dataRange[2])
        endDay   = (curDate - datetime.timedelta(days=dataRange[0])).strftime(dataRange[2])
        return startDay, endDay

    # Internal function in config file
    startDay, endDay =  _setStartEndTime (e=1, s=1000, f="%Y%m%d")
    config.QUERY_PARAMS = {
            "$start" : startDay,
            "$end"   : endDay
    }


    if __name__ == '__main__':
        parser = argparse.ArgumentParser(description='Loading data from json files, cant get: source list files or destination list files or append mode () ')
        dd = dingDong(  dicObj=None, filePath=None, dirData=PROJECT_FOLDER,
                        includeFiles=FILES_INCLUDE, notIncludeFiles=FILES_NOT_INCLUDE,
                        dirLogs=LOGS_FOLDER, connDict=CONN_DICT, processes=4)

        dd.setLoggingLevel(val=logging.DEBUG)

        dd.ding()
        dd.msg.addState("DING FINSHED")

        dd.dong()
        dd.msg.addState("DONG FINISHED")

        dd.execDbSql(queries=ddSQLExecution, connName='sql')
        dd.msg.addState("DONE SQL QUERIES")

        dd.execMicrosoftOLAP(serverName=<SSAS server name>, dbName=<SSAS db name>, cubes=[], dims=[], fullProcess=True)
        dd.msg.addState("DONOE MICROSOFT SSAS")

        dd.msg.sendSMTPmsg (msgName="JOB SAMPLE LOADING FINSISHED", onlyOnErr=False, withErr=True, )

Ding Work-flow
--------------

:EXTRACT: Load from oracle query into sql server table  **STG_Services** using truncate insert method
:EXECUTE: Executing SQL file named ** update_Target_STG_Services.sql **
:EXTRACT: Merge data from table ** STG_Services ** (target) to ** DWH_Services **
:TRANFORM: function fDCast(). Columns ValidEndDate,ValidBgDate convert string values to smalldatetime
            More on function can be found at :ref:`tag_functions`
:TRANSFORM: execution function. Column LongDesc Concatinate 3 columns into long string: COL6+COL7+COL8
:TRANSFORM: function fDCurr(). Update Column ETL_Date with system datetime value.
:EXTRACT: Merge data from **STG_Services** into **DWH_Services**

  * merge key columns: "COL1","COL2"
  * merge using connection functionality and can be done only if source and target are located at the same connection

Dong Work-Flow
--------------

:DATA-TYPES: All oracle query columns COL1, COL2, ... will be in **STG_Services** and **DWH_Services** using
SQL datatype align to oracle data-types
:DATA-TYPES: ValidEndDate,ValidBgDate will have smalldatetime
:DATA-TYPES: LongDesc will have nvarchar(500)
:DATA-TYPES: ETL_Date will have smalldatetime
:INDEX: Tables **STG_Services** and **DWH_Services** will have non unique ("iu":false), clustered index ("ic":true) on COL1 and COl2
