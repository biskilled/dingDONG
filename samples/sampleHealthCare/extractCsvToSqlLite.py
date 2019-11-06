""" import modules -> logging used fr setting log level"""
import logging
from dingDong import DingDong
from dingDong import Config

""" set log level: logging.INFO, logging.DEBUG, logging.ERROR """
Config.LOGS_DEBUG = logging.DEBUG
Config.VERSION_DIR = "C:\\dingDong"

""" Config all connection URL
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
    'sqlite': {"url":"C:\\dingDong\\sqlLiteDB.db","create":"tableName"}}

""" This is sample JSON configuration format for:
    1. mapping and loading CSV file named DATAELEMENTDESCRIPTION into SQLLite table named dateElements_Desc
    2. mapping and loading CSV file named DEMOGRAPHICS into SQLLite table named demographics
    3. mapping and loading CSV file named MEASURESOFBIRTHANDDEATH into SQLLite table named birthDate
    4. create a new query based on demographics and birthDate  into new table named Final
    5. Update sample field at the Final table by using direct PL/SQL query
    6. Extract Final table data into a CSV file 

    file default datatype can be found at dingDong.conn.baseBatch under DEFAULTS values (currently set to VARCHAR(200) for all relation Dbs   
"""
source : ["s"]

nodesToLoad = [
    {"source": ["file", "DATAELEMENTDESCRIPTION.csv"],
     "target": ["sqlite", "dateElements_Desc"]},

    {"source": ["file", "DEMOGRAPHICS.csv"],
     "target": ["sqlite", "demographics"]},

    {"source": ["file", "MEASURESOFBIRTHANDDEATH.csv"],
     "target": ["sqlite", "birthDate"]},

    {"query": ["sqlite", """   Select d.[State_FIPS_Code] AS A, d.[County_FIPS_Code] AS B, d.[County_FIPS_Code] AS G,d.[County_FIPS_Code], d.[CHSI_County_Name], d.[CHSI_State_Name],[Population_Size],[Total_Births],[Total_Deaths]
                                    From demographics d INNER JOIN birthDate b ON d.[County_FIPS_Code] = b.[County_FIPS_Code] AND d.[State_FIPS_Code] = b.[State_FIPS_Code]"""],
     "target": ["sqlite", "Final", -1]},

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
        1. copy old data into a table with date prefix and create a new table with updated metadata (default, CODE:-1)
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


