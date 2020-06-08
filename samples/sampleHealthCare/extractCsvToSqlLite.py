""" import modules -> logging used fr setting log level"""
import logging
from dingDONG import dingDONG
from dingDONG import Config

""" set log level: logging.INFO, logging.DEBUG, logging.ERROR """
Config.LOGS_DEBUG = logging.DEBUG

""" SMTP Configuration for sending pipeLine status by Email """
Config.SMTP_RECEIVERS = []      #'pipe@line.com' ...
Config.SMTP_SERVER = ""         # 'smtp.gmail.com '
Config.SMTP_SERVER_USER = ""
Config.SMTP_SERVER_PASS = ""
Config.SMTP_SENDER  = ""


""" Config all connection URL
    key : An internal connection name, or connection type (sql, oracle, file .. )
    values: 
        String--> connection URL, SAMPLE: "sql":"<Sql server connection string>;UID=USER;PWD=PWD;"
        Dictionary --> 
            'conn'  -> connenction type. support: 'sql', 'oracle', 'access', 'mysql', 'sqllite', 'mongo' ...  
            'url'   -> connection string URL
            'filter'-> filter objects
            ... additional paramters can be found under   
"""
Config.CONNECTIONS = {
    'file': {"url":"C:\\dingDONG\\data\\", "csv":True, "change":1},
    'sqlite': {"url":"C:\\dingDONG\\data\\sqlLiteDB.db"}
}

""" pipeline sample JSON configuration:
    1. MAP and LOAD DEMOGRAPHICS CSV file into SQLLite US_demographics table 
    2. MAP and LOAD MEASURESOFBIRTHANDDEATH CSV file into SQLLite US_measures_birth_death table
    3. MAP and LOAD VUNERABLEPOPSANDENVHEALTH CSV file into SQLLite US_global_measures table
    3. mapping and loading CSV file named MEASURESOFBIRTHANDDEATH into SQLLite table named birthDate
    4. create a new query based on demographics and birthDate  into new table named Final
    5. Update sample field at the Final table by using direct PL/SQL query
    6. Extract Final table data into a CSV file 

    file default datatype can be found at dingDong.conn.baseBatch under DEFAULTS values (currently set to VARCHAR(200) for all relation Dbs   
"""

simplePipeLine = [
    {"source": ["file", "DEMOGRAPHICS.csv"],
     "target": ["sqlite", "US_demographics"]},

    {"source": ["file", "MEASURESOFBIRTHANDDEATH.csv"],
     "target": ["sqlite", "US_measures_birth_death"]},

    {"source": ["file", "VUNERABLEPOPSANDENVHEALTH.csv"],
     "target": ["sqlite", "US_global_measures"]}
]

"""
    Init DingDONG class
        dicObj -> data pipeline dictionary 
        dirData-> directory for all JSON pipelines 
        includeFiles    -> using specific defined JSON files located at dirData 
        notIncldeFiles  -> ignore specific defined JSON files located at dirData 
        connDict        -> set Config.CONNECTIONS property 
        processes       -> data loading maximum parrallel processing (DONG module) 
"""

dd = dingDONG(dicObj=simplePipeLine, filePath=None, dirData=None, includeFiles=None,notIncludeFiles=None,connDict=None, processes=1)

""" DING -> manage target strucuture
    create target if not exists. 
    compare source strucuture to existing target strucure and update accordingly using 3 methods :
    - defualt: save old strucutre with old data and create new strucure
    - update:  update existing strucure
    - no update: target object is not chamges  
"""
dd.ding()

""" DONG: Load, maniulate and merge data 
    "stt":      manage target data type, target naming, add calculaed columns  
    "column":   target column types 
    "mapping":  map source column to target column 
"""
dd.dong()

""" Add calculated column into all tables """
advancedPipeLine = [
    {"source": ["file", "DEMOGRAPHICS.csv"],
     "target": ["sqlite", "US_demographics"],
     "sttappend":{"Strata_Determining_Factors":{"f":"fR(', ','-> ')"},
                  "LOCATION_DESC": {"t": "nvarchar(128)", "e": "{CHSI_State_Name} -> {CHSI_County_Name}"},
                  "LOCATION_CODE": {"t": "nvarchar(128)", "e": "{State_FIPS_Code}{County_FIPS_Code}"},
                  "etlDate":{"t":"smalldatetime","f":"fDCurr()"},
                 }
     },
    {"source": ["file", "MEASURESOFBIRTHANDDEATH.csv"],
     "target": ["sqlite", "US_measures_birth_death"],
     "sttappend": { "LOCATION_CODE": {"t": "nvarchar(128)", "e": "{State_FIPS_Code}{County_FIPS_Code}"},
                    "etlDate": {"t": "smalldatetime", "f": "fDCurr()"}}
     },
    {"source": ["file", "VUNERABLEPOPSANDENVHEALTH.csv"],
     "target": ["sqlite", "US_global_measures"],
     "sttappend": { "LOCATION_CODE": {"t": "nvarchar(128)", "e": "{State_FIPS_Code}{County_FIPS_Code}"},
                    "etlDate": {"t": "smalldatetime", "f": "fDCurr()"}}
     },
]

dd.Set(dicObj=advancedPipeLine)
dd.msg.addState("DING: update csv files schema  ")
dd.ding()
dd.msg.addState("DONG: load new CSV structure  ")
dd.dong()

businessLogic = [
    {
        "query": ["sqlite", """   
            Select d.LOCATION_CODE, d.Population_Size, d.Poverty, d.Age_19_Under, d.Age_19_64, Age_65_84, 
                    bd.LBW, bd.Stroke, bd.Suicide, bd.Total_Births, bd.Total_Deaths, g.Unemployed, g.Major_Depression
            From US_demographics d  LEFT OUTER JOIN US_measures_birth_death bd ON d.LOCATION_CODE=bd.LOCATION_CODE
                LEFT OUTER JOIN US_global_measures g ON d.LOCATION_CODE=g.LOCATION_CODE"""],
         "target": ["sqlite", "FINAL" ],
        "sttappend": { "etlDate": {"t": "smalldatetime", "f": "fDCurr()"}}
    },
    {
        "myexec": ["sqlite", "UPDATE FINAL SET Suicide='UnKnown' WHERE Suicide<0;"]
    },
    {
        "source": ["sqlite", "FINAL"],
        "target": ["file", "final.csv"]
    }
]

### Add business logic
dd.Set(dicObj=businessLogic)
dd.msg.addState("DING: update business logic tables  ")
dd.ding()
dd.msg.addState("DONG: load business logic tables   ")
dd.dong()
dd.msg.end(pr=True)
dd.msg.sendSMTPmsg(msgName="FINISH dingDONG pipeLine !", onlyOnErr=True, withErr=True, withWarning=True)
