|PyPI version| |Docs badge| |License|

*********
Ding Dong
*********

dingDong created for modeling devloping and mainting complex data integration projects - relational database
or cloud APIs integration, data cleansing or modeling algorithms.

The project is currently supporting batch loading from diverse RMDBs. we do plan to extand it for a full support
in REST and websockecet as well. The project if fully developer in python, we do use Node for our REST integration.

dingDong developeed to use as a glue between diverse data storage types using each componenet best of practice.
for example there is no JOIN or UNION implementation becouse usually this functionaity is used in much efficient way at the connectors.
we focus on managin the meta-data correctly, and helping creating fast and eaty to manage data workflows.

Using the capabiltes of existing connectors with dingDong allow us to create robust data project using the
adavatges of all the componenents

dingDong is splitted into two main moduls:

- DING - create and manage overall metadata strucutre for all object listed in the workflow
    - creating new objects
    - modify existing object by using back propogation mechanism
    - update data into new object
    - store old structure
    - (todo) --> truck all changes in main repo for CI/CD processess

- DONG - extract and load data from diverse connectors
    - extract data - support multithreading for extracting massive data volume
    - transfer     - enable to manipylate date by adding manipulation function on column
                   - enable to add custom calculated fields
    - merge        - merging source with target data can be done if source and merge located at the same connector
    - exec         - enable to execute PL/SQL or sstored procedure command as part of the whole data workflow

Read more about dingDong at http://www.biSkilled.com (marketing) or at `dingDong documentation <https://dingdong.readthedocs.io/en/latest>`_

Installation
============
`download from GitHub <https://github.com/biskilled/dingDong>`_ or install by using ``pip``::

    pip install dingDong

Samples
=======
download samples CSV files DATAELEMENTDESCRIPTION.csv, DEMOGRAPHICS.csv, MEASURESOFBIRTHANDDEATH.csv
located at `samples/sampleHealthCare/csvData <samples/sampleHealthCare/csvData/>`_ folder.
In this sample we use *C:\\dingDong* as our main folder

the sample demonstrate how to load three csv files into sqllite, create a simple qury based
on that tables and send the result into new CSV file.

1. load module and basic configuration
   Config.CONN_URL - set connection URl into all connectors
   key : General connection name or connection type (sql, oracle, file .. )
   value can be string or dictionary:
       String      --> Connection string URL (key defined connection type: sql, oracle, mySql....)
       Dictionary  --> must have 'conn' (connection type) and 'url' (connection string)
       available connection can be found at dingDong.misc.enumsJson.eConn

    Config.LOGS_DEBUG   -> set logging level (logging.DEBUG, logging.WARNING...)
    Config.LOGS_DIR     -> set logs directory for creating logs files

    confgiuration properties can be found at `dingDong documentation <https://dingdong.readthedocs.io/en/latest>`_

::

    from dingDong import DingDong
    from dingDong import Config

    Config.CONN_URL = {
        'x1'    : {'conn':'sql',"url":"DRIVER={SQL Server};SERVER=CPX-VLQ5GA42TW2\SQLEXPRESS;DATABASE=ContosoRetailDW;UID=bpmk;PWD=bpmk;"},
        'x2'    : {'conn':'sql',"url":"DRIVER={SQL Server};SERVER=CPX-VLQ5GA42TW2\SQLEXPRESS;DATABASE=ContosoRetailDW;UID=bpmk;PWD=bpmk;"},
        'file'  : "C:\\dingDong\\",
        'sqlite': "C:\\dingDong\\sqlLiteDB.db"}
    Config.LOGS_DEBUG = logging.DEBUG
    Config.LOGS_DIR = "C:\\dingDong"

2.  Creating workflow - workflow can be done as JSON format or python dictiaries
    In this sample we will use python dicionary the sample workflow contain:
    -  mapping and loading CSV file named DATAELEMENTDESCRIPTION into sqllite table named dateElements_Desc
    -  mapping and loading CSV file named DEMOGRAPHICS into sqllite table named demographics
    -  mapping and loading CSV file named MEASURESOFBIRTHANDDEATH into sqllite table named birthDate
    -  create a new query based on demographics and birthDate  into new table named Finall
    -  Update sample field at Finall table by using direct PL/SQL query
    -  Extract Finall table data into a CSV file
        We use VARCHAR(200) as default CSV column datatype. configuration can be found at DEFAULTS locatec at dingDong.conn.baseBatch
::

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

3.  Init class dingDong
    - dicObj      -> loading dicionary as a workflow
    - dirData     -> loading JSON files in this folder
    - includeFiles-> FILTER files to load in dirData folder
    - notIncldeFiles-> Ignoring files to load in dirData folde
    - connDict    -> equal to Config.CONN_URL, st connection Urls
    - processes   -> number of parrallel processing, used only for loading data (DONG module)
::

    m = DingDong(dicObj=nodesToLoad,
                 filePath=None,
                 dirData=None,
                 includeFiles=None,
                 notIncludeFiles=None,
                 connDict=None,
                 processes=1)

4.  DING
    - creating dateElements_Desc, demographics and birthDate tables based on CSV files
    - creating Finall table based on defined query

    if table exists and strucure changed - Ding module will track chnages by duplicate object with data and create new object schema
::

    m.ding()

5.  DONG - Extracting data from CSV files into sqlLite table. defoult loading is truncate-> insert method
    Extract data from query into Finall table (truncate-> insert )
    if object strucuture changed and mode 2
        - history table will be created
        - new object will be create and will populated with data from history table (identical column name)
::

        m.dong()

Full sample code::

    from dingDong import DingDong
    from dingDong import Config

    Config.CONN_URL = {
        'x1'    : {'conn':'sql',"url":"DRIVER={SQL Server};SERVER=CPX-VLQ5GA42TW2\SQLEXPRESS;DATABASE=ContosoRetailDW;UID=bpmk;PWD=bpmk;"},
        'x2'    : {'conn':'sql',"url":"DRIVER={SQL Server};SERVER=CPX-VLQ5GA42TW2\SQLEXPRESS;DATABASE=ContosoRetailDW;UID=bpmk;PWD=bpmk;"},
        'file'  : "C:\\dingDong\\",
        'sqlite': "C:\\dingDong\\sqlLiteDB.db"}
    Config.LOGS_DEBUG = logging.DEBUG
    Config.LOGS_DIR = "C:\\dingDong"

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

    m = DingDong(dicObj=nodesToLoad,
                 filePath=None,
                 dirData=None,
                 includeFiles=None,
                 notIncludeFiles=None,
                 connDict=None,
                 processes=1)
    m.ding()
    m.dong()

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

+-------------------+------------------+------------------+-------------+------------------------------------------+
| connectors Type   | python module    | checked version  | dev status  | notes                                    |
+===================+==================+==================+=============+==========================================+
| sql               |  pyOdbc          | 4.0.23           | tested, prod| slow to extract, massive data volumne    |
|                   |                  |                  |             | preffered using ceODBC                   |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| sql               | ceODBC           | 2.0.1            | tested, prod| sql server conn for massive data loading |
|                   |                  |                  |             | installed manualy from 3rdPart folder    |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| access            | pyOdbc           | 4.0.23           | tested, prod|                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| oracle            | cx-oracle        | 6.1              | tested, prod|                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| CSV / text files  | CSV / CSV23      | 0.1.5            | tested, prod|                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| mysql             | pyMySql          | 0.6.3rc1         | dev         |                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| vertica           | vertica-python   | 0.9.1            | dev         |                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| sqllite           | sqllite3         | 6.1              | tested, prod|                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| mongoDb           | pyMongo          | 3.7.2            | dev         |                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| salesforce        | simple_salesforce| 3.7.2            | dev         |                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| haddop/Hive       | .                | .                | dev         |                                          |
+-------------------+------------------+------------------+-------------+------------------------------------------+


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
   