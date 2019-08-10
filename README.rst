|PyPI version| |Docs badge| |License|

*********
Ding Dong
*********

dingDong created for modeling devlop and maintin complex data integration projects - relational database
or cloud APIs, Sql or no sql, data cleansing or modeling algorithms.

The project is currently support and batch loading, our next phase is to extend it for REST and websockecet
APIs itegration as well.

Current project is purely python based. REST and websocket will be implemnted by nodeJs.

This project aims to use as a glue between diverse data storage types.
we did not implementeed any JOIN or UNION method which can be used in much efficient way at the connectoer themself
we did impleented pure meta data manager, data tranformation and extracting method.
Using the capabiltes of existing connectors with dingDong allow us to create robust data project using the
adavatges of all the componenents

Connectors :
        Sql server  - tested, ready for production
        Oracle      - tested, ready for production
        SqlLite     - tested, ready for production
        text files  - tested, ready for production
        CSV         - tested
        Vertica     - partially tested
        MySql       - partially tested
        MongoDB     - partially tested
        Hadoop/Hive - not implemted

        API Support
        SalesForce  - partially, not tested


dingDong is splitted to two main moduls:
 - DING - create and manage overall metadata strucutre for all object at the workflow
         - creating new object
         - modify existing object by using back propogation mechnism
         - update data into now object
         - store old strucure
         - (todo) --> truck all changes in main repo for CI/CD processess

- DONG - extract and load data from diverse realtion / not relational connectors
    - extract data - support multithreading for extracting massive data volume
    - transfer     - enable to manipylate date by adding manipulation function on column
                   - enable to add custom calculated fields
    - merge        - merging source with target data can be done if source and merge located at the same connector
    - exec         - enable to execute PL/SQL or sstored procedure command as part of the whole data workflow

Read more about dingDong at http://www.biSkilled.com (marketing) or at `dingDong documentation <https://readthedocs.org/projects/popeye-etl/>`_

Installation
============
`download from GitHub <https://github.com/biskilled/dingDong>`_ or install by using ``pip``::

    pip install dingDong

Samples
=======
download samples CSV files DATAELEMENTDESCRIPTION.csv, DEMOGRAPHICS.csv, MEASURESOFBIRTHANDDEATH.csv
located at `samples/sampleHealthCare/csvData <samples/sampleHealthCare/csvData/>`_ folder
In this sample we use *C:\\dingDong* as our main folder

In this sample will load 3 files in sqlLite, create query to load a new report into at table, and extract all data into CSV files

Full sample code ::

    from dingDong import DingDong
    from dingDong import Config

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
        'x1'    : {'conn':'sql',"url":"DRIVER={SQL Server};SERVER=CPX-VLQ5GA42TW2\SQLEXPRESS;DATABASE=ContosoRetailDW;UID=bpmk;PWD=bpmk;"},
        'x2'    : {'conn':'sql',"url":"DRIVER={SQL Server};SERVER=CPX-VLQ5GA42TW2\SQLEXPRESS;DATABASE=ContosoRetailDW;UID=bpmk;PWD=bpmk;"},
        'file'  : "C:\\temp\\",
        'sqlite': "C:\\temp\\sqlLiteDB.db"}


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
            {   "source":["file","DATAELEMENTDESCRIPTION.csv"],
                "target":["sqlite","dateElements_Desc"]},

            {   "source":["file","DEMOGRAPHICS.csv"],
                "target":["sqlite","demographics"]},

            {   "source":["file","MEASURESOFBIRTHANDDEATH.csv"],
                "target":["sqlite","birthDate"]},

            {   "query":["sqlite","""   Select d.[State_FIPS_Code] AS A, d.[County_FIPS_Code] AS B, d.[County_FIPS_Code] AS G,d.[County_FIPS_Code], d.[CHSI_County_Name], d.[CHSI_State_Name],[Population_Size],[Total_Births],[Total_Deaths]
                                        From demographics d INNER JOIN birthDate b ON d.[County_FIPS_Code] = b.[County_FIPS_Code] AND d.[State_FIPS_Code] = b.[State_FIPS_Code]"""],
                "target":["sqlite","Finall", 2]},

            {   "myexec":["sqlite","Update dateElements_Desc Set [Data_Type] = 'dingDong';"]},

            {   "source":["sqlite","Finall"],
                "target":["file","finall.csv"]}
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

    m = DingDong(dicObj=nodesToLoad,
                 filePath=None,
                 dirData=None,
                 includeFiles=None,
                 notIncludeFiles=None,
                 connDict=None,
                 processes=1)

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
    m.dong()

Quick explain :

1. import dingDong main modules
2. set connection URL into Config.CONN_URL property
3. nodesToLoad is a list of dictionary object to load.
   full availabe key list can be found at dingDong documantion
4. Init dingDong class
5. DING - mapping modulre
6  DONG - extract and load module




Road map
========

We would like to create a platform that will enable to design, implement and maintenance and data integration project such as:

*  Any REST API connectivity from any API to any API using simple JSON mapping
*  Any Relational data base connectivity using JSON mapping
*  Any Non relational storage
*  Main platform for any middleware business logic - from sample if-than-else up to statistics algorithms using ML and DL algorithms
*  Enable Real time and scheduled integration

We will extend our connectors and Meta-data manager accordingly.

BATCH supported connectors
==========================

+-------------------+---------------+------------------+---------------------------------------------+
| connectors Type   | python module | checked version  | notes                                       |
+===================+===============+==================+=============================================+
| sql               |  pyOdbc       | 2.0.1            | slow to extract, prefered to use ceODBc     |
+------------------ +---------------+------------------+---------------------------------------------+


==================== ==================== ====================
   connectors Type       python module       checked version
-------------------- -------------------- --------------------
    sql                  pyodbc or ceODBC    2.0.1 / 2.1
==================== ==================== ====================


*  APIs       : Salesforce
*  RMDBs      : Sql-Server, Access, Oracle, Vertice, MySql
*  middleware : column transformation and simple data cleansing
*  DBs        : mongoDb
*  Batch      : Using external scheduler currently .....
*  onLine     : Needs to be implemented .....

Authors
=======

dingDong was created by `Tal Shany <http://www.biskilled.com>`_
(tal@biSkilled.com)
We are looking for contributions !!!

License
=======

GNU General Public License v3.0

See `COPYING <COPYING>`_ to see the full text.

.. |PyPI version| image:: https://img.shields.io/pypi/v/dingDong.svg
   :target: https://github.com/biskilled/dingDong
.. |Docs badge| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :target: https://readthedocs.org/projects/dingDong/
.. |License| image:: https://img.shields.io/badge/license-GPL%20v3.0-brightgreen.svg
   :target: COPYING
   :alt: Repository License
   