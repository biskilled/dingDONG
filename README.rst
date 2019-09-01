|PyPI version| |Docs badge| |License|

*************
Ding Dong ETL
*************

Ding-Dong is a meta-data management infrastructure for ETL and REST full data integration projects.
using Ding-Dong help to design, develop, maintain and scale complex batch and real-time data projects.

The use ding-dong propagation mechanism allowing developers to build complex data workflows in a fraction of time

See `Ding-Dong documentation <https://dingdong.readthedocs.io/en/latest>`_ for install and developer documentation.
Ding-Dong at http://www.biSkilled.com (marketing) or at

Why Ding-Dong?
==============
**Simple, because we are lazy!**

Data project usually starts with a source to target modeling and implementing loading by using some ETL tool. during the project life-cycle, we have to change constantly source to target mapping and update our ETL infrastructure accordingly. Usually, this process is done in an iterative process and consume time and efforts. In addition, common ETL tools based on XML scheme which is hard to manage in full automated CI/CD processes.

Ding-Dong is used to solve that by aligning design and mapping to load and data extraction. It is possible to manage all mapping schema in a convenient format such as excel or CSV and Ding-Dong will translate it into Ding-Dong JSON workflows format for the Loading and extraction methods. any update in source to target mapping will be propagated to all related object (mirroring, STG, and DWH object).

Maintaining business logic in Ding-Dong is used directly by using SQL files that can be executed at the IDE as well. We added Ding-Dong **comments** syntax that use to extract queries from SQL files which help split maintain all business logic in manageable SQL files

Ding-Dong developed to use as a glue between diverse data storage types using each component best of practice. We did not focus on implementing faster JOIN or UNION function but we did focus on managing the meta-data correctly.

Using the native capabilities of existing connectors with Ding-Dong allow us to create robust data project using the advantages of all the components

Ding-Dong has two main modules:

- DING - create and manage overall meta-data structure for all object listed in the work-flow
    - creating new objects
    - modify an existing object by using a back-propagation mechanism
    - update data into new object
    - store old structure
    - (Todo) --> truck all work-flow changes as part of full CI/CD methodology

- DONG - extract and load data from diverse connectors
    - extract data - support multithreading for extracting massive data volume
    - transfer     - enable to add a custom function on existing columns
                   - enable to add custom calculated fields
    - merge        - merging source with target data can be done if the source and merge located at the same connector
    - exec         - enable to execute PL/SQL or stored procedure command as part of the whole data workflow



Installation
============
`download from GitHub <https://github.com/biskilled/dingDong>`_ or install by using ``pip``::

    pip install dingDong

Samples
=======
download samples CSV files DATAELEMENTDESCRIPTION.csv, DEMOGRAPHICS.csv, MEASURESOFBIRTHANDDEATH.csv
located at `samples/sampleHealthCare/csvData <https://github.com/biskilled/dingDong/tree/master/samples/sampleHealthCare/csvData>`_ folder.
In this sample, we use **C:\\dingDong** as our main folder for all source CSV files and dingDong logs.

Full code sample **extractCsvToSqlLite.py** located at `samples/sampleHealthCare/ <https://github.com/biskilled/dingDong/tree/master/samples/sampleHealthCare>`_ folder

the sample demonstrates how to load three CSV files into SqlLite, create a simple query-based
on those tables and send the result into a new CSV file.

configuration properties can be found at `dingDong documentation <https://dingdong.readthedocs.io/en/latest>`_

::

    import logging
    from dingDong import DingDong
    from dingDong import Config

    """ Main configuration """

    """ set log level: logging.INFO, logging.DEBUG, logging.ERROR """
    Config.LOGS_DEBUG = logging.DEBUG

    """
        set connection URL into all connectors
        key   -> general connection name or connection type (sql, oracle, file .. )
        value -> can be string or dictionary
            String     --> Connection string URL (key defined connection type: sql, oracle, mySql....)
            Dictionary --> must have 'conn' (connection type) and 'url' (connection string).
        available connection can be found at dingDong.misc.enumsJson.eConn

    """
    Config.CONN_URL = {
        'sampleSql': {'conn': 'sql',"url": "<Sql server connection string>;UID=USER;PWD=PWD;"},
        'file': "C:\\dingDong\\",
        'sqlite': "C:\\dingDong\\sqlLiteDB.db"}

2. Creating work flow can be done as JSON format or python dictionaries
   In this followed sample we will use python dictionary the sample work flow contain

* mapping and loading CSV file named DATAELEMENTDESCRIPTION into SqlLite table named dateElements_Desc
* mapping and loading CSV file named DEMOGRAPHICS into SqlLite table named demographics
* mapping and loading CSV file named MEASURESOFBIRTHANDDEATH into SqlLite table named birthDate
* create a new query based on demographics and birthDate  into new table named Final
* Update sample field at Final table by using direct PL/SQL query
* Extract Final table data into a CSV file.
  We use VARCHAR(200) as default CSV column data type. configuration can be found at DEFAULTS under dingDong.conn.baseBatch

::

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

3. Init class dingDong

:dicObj:        loading dictionary as a work flow
:dirData:       loading JSON files in this folder
:includeFiles:  FILTER files to load in dirData folder
:notIncldeFiles: Ignoring files to load in dirData folder
:connDict:      equal to Config.CONN_URL, st connection Urls
:processes:     number of parallel processing, used only for loading data (DONG module)

::

    m = DingDong(dicObj=nodesToLoad,
                 filePath=None,
                 dirData=None,
                 includeFiles=None,
                 notIncludeFiles=None,
                 connDict=None,
                 processes=1)

4. DING

* creating dateElements_Desc, demographics and birthDate tables based on CSV files
* creating Final table based on the defined query

 if the table exists and structure changed - Ding module will track changes by a duplicate object with data and create new object schema

::

    m.ding()

5.  DONG - Extracting data from CSV files into SQLite table. default loading is truncate-> insert method
    Extract data from a query into the Final table (truncate-> insert )

* if object structure changed and mode 2 (like at the sample)
  * history table will be created
  * new object will be created and will be populated with data from history table (identical column name)

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
                "target":["sqlite","Final", 2]},

            {   "myexec":["sqlite","Update dateElements_Desc Set [Data_Type] = 'dingDong';"]},

            {   "source":["sqlite","Final"],
                "target":["file","final.csv"]}
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
*  Any Relational database connectivity using JSON mapping
*  Any Non-relational storage
*  Main platform for any middleware business logic - from sample if-than-else up to statistics algorithms using ML and DL algorithms
*  Enable Real-time and scheduled integration

We will extend our connectors and Meta-data manager accordingly.

BATCH supported connectors
==========================

+-------------------+------------------+------------------+-------------+------------------------------------------+
| connectors Type   | python module    | checked version  | dev status  | notes                                    |
+===================+==================+==================+=============+==========================================+
| sql               |  pyOdbc          | 4.0.23           | tested, prod| slow to extract, massive data volume     |
|                   |                  |                  |             | preferred using ceODBC                   |
+-------------------+------------------+------------------+-------------+------------------------------------------+
| sql               | ceODBC           | 2.0.1            | tested, prod| sql server conn for massive data loading |
|                   |                  |                  |             | installed manually from 3rdPart folder   |
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

See `COPYING <https://github.com/biskilled/dingDong/blob/master/COPYING>`_ to see the full text.

.. |PyPI version| image:: https://img.shields.io/pypi/v/dingDong.svg
   :target: https://github.com/biskilled/dingDong
.. |Docs badge| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :target: https://readthedocs.org/projects/dingDong/
.. |License| image:: https://img.shields.io/badge/license-GPL%20v3.0-brightgreen.svg
   :target: COPYING
   :alt: Repository License
