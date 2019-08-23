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

""" This is sample JSON configurtion formt for:
    1. mapping and loading CSV file named DATAELEMENTDESCRIPTION into sqllite table named dateElements_Desc
    2. mapping and loading CSV file named DEMOGRAPHICS into sqllite table named demographics
    3. mapping and loading CSV file named MEASURESOFBIRTHANDDEATH into sqllite table named birthDate
    4. create a new query based on demographics and birthDate  into new table named Finall
    5. Update sample field at Finall table by using direct PL/SQL query
    6. Extract Finall table data into a CSV file 

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
     "target": ["sqlite", "Finall", 2]},

    {"myexec": ["sqlite", "Update dateElements_Desc Set [Data_Type] = 'dingDong';"]},

    {"source": ["sqlite", "Finall"],
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

m = DingDong(dicObj=nodesToLoad, filePath=None, dirData=None,
             includeFiles=None,notIncludeFiles=None,connDict=None, processes=1)

m.msg.addState("Start Ding")

""" Mapping files strucutre into table strucure
    Target not exists   -> create new target table based on source table definitions 
    Target exists       -> if there is change, there are 3 option to update target table structure  
        1. copy old data into table with date prefix and create new table with updated meta data (default, CODE:-1)
        2. create new table schema, store old schema in copied table with date prefix and merge data from old strucute into new strucure (CODE: 1, updteted at taret or merge key values)
        3. no change can be made into this table. CODE number 2. can be added only to target or merge objects    
"""
m.ding()

""" Extracting and loading data from source to target or to merge
    if stt node exists in JSOn mapping -> will update fields accrodinly 
    if column node exists -> will map column types by column node definitin
    if mapping node exists-> will map source to target accordinglr

    more detild can be found at decumentation 
"""
m.msg.addState("Start Dong")
m.dong()

m.msg.end(msg="FINISHED",pr=True)

