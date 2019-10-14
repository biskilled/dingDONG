|PyPI version| |Docs badge| |License|

******************************
Ding Dong data preparation ETL
******************************

dingDong is a metadata management infrastructure for data preparation, ETL, and machine learning models
with a complete version control support for all data preparation, loading, and ML experiments.

using dingDONG propagation mechanism help to design, develop, maintain and scale complex batch and real-time data projects in a fraction of time

See `Ding-Dong documentation <https://dingdong.readthedocs.io/en/latest>`_ for install and developer documentation.
More information can be found at http://www.biSkilled.com

Why Ding Dong?
==============
**Simple, because it is fast and easy to implement !**

Steps for creating at data project:

- Source to target modeling - mostly done by excel or external ERD mapping tools
- Implementing - done mainly by external ETL tools
- Improvements, extensions, maintenance and deploy between environments - usually an iterative process which consumes time and effort. meta-data is constantly changing. if business logic involves ML/DL models - versioning must be set for trucking overall results accuracy

**dingDONG developed to maintain and speed the overall data project implementation**

- Source to target modeling - dingDONG truck meta-data changes such as adding, removing, updating DB columns. converting data-type from diverse connections or adding key sequence is done automatically. Modeling can be managed in a simple excel file, dindDONG will truck excel updates and will update all object by using a propagation mechanism.
- Implementing E(T)L (extract, transform, load) - dingDONG multi-threading and data fetching optimization give a powerful tool for massive bulk data loading
- Transform - It is possible to add calculated columns or manipulate existing column by adding python functions. A function such as scaling or normalization can be added as easy as adding the current date or concatenate functions
- Business logic - Maintaining business logic in dingDONG is used directly by using SQL files that can be executed directly at the development IDE. Using dingDONG **comments** syntax is used to extract and execute complex queries from SQL files which allows managing all business logic in separate files.
- Version control - dingDONG version control and management keeps track of all metadata changes and stored it in a proprietary dingDONG DB (MongoDB). dingDONG supports **GIT** version-control mechanism for managing ML/DL projects, versioning and tracking experiments history results. There is full support in **GITHUB** for distributed versioning.

dingDONG modules:

- DING - create and manage overall meta-data structure for all listed object in the work-flow
    - creating new objects
    - modify an existing object by using a back-propagation mechanism
    - update data into new object
    - save old structures
    - CI source control

- DONG - extract and load data from diverse connectors
    - extract data - support multi-threading for extracting massive data volume
    - transfer     - add a custom function on existing columns or new custom calculated fields
    - merge        - merging source with the target object (supported only for objects at the same connector)
    - exec         - execute PL/SQL/Stored procedure commands

- VERSION CONTROL - track meta-data and code updates
    - Versions numbers are managed automatically (sample: <major-release>.<minor-release>.<fixId>)
    - Any change at object metadata is automatically stored at dingDONG repository
    - Support managed GIT versioning of ML / DL experiments.
       - Code update - a new version is created
       - Experiment results stored in dingDong repository for future analytics
       - Roll revisions history by version number
       - Define measures such as counting total input or total output rows
       - Store executions result for compare experiments performance

.. image:: https://github.com/biskilled/dingDong/blob/master/docs/_static/dingDongOneSlide.jpg
   :alt: dingDONG architecture


Installation
============

`download from GitHub <https://github.com/biskilled/dingDong>`_ or install by using ``pip``::

    pip install dingDong

Adding MongoDB for pip installation is now cooked. Installation instruction will come soon
Docker support - as well, still at the oven .. will come soon

Samples
=======

Samples can be found under `Ding-Dong sample documentation <https://dingdong.readthedocs.io/en/latest/rst/samples.html>`_

The sample below loads 3 CSV files into SQLite. (we are using SQL to merge this CSV and extract data into a report table and CSV file).

Download ZIP file with 3 samples CSV files DATAELEMENTDESCRIPTION.csv, DEMOGRAPHICS.csv, MEASURESOFBIRTHANDDEATH.csv
located at `samples/sampleHealthCare/csvData.zip <https://github.com/biskilled/dingDong/raw/master/samples/sampleHealthCare/csvData.zip>`_ folder.
In this sample, we use **C:\\dingDONG** as our main folder for all source CSV files and dingDong logs.

Full code sample **extractCsvToSqlLite.py** located at `samples/sampleHealthCare/ <https://github.com/biskilled/dingDong/tree/master/samples/sampleHealthCare/extractCsvToSqlLite.py>`_ folder

The sample demonstrates how to load 3 CSV files into SqlLite, create a simple query-based
on those tables and send the result to a new CSV file.

configuration properties can be found at `dingDONG documentation <https://dingdong.readthedocs.io/en/latest>`_

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

2. Creating workflow can be done as JSON format or python dictionaries
   For the sake of this example, we will use a python dictionary. The sample work-flow will contain:

* mapping and loading CSV file named DATAELEMENTDESCRIPTION into SQLLite table named dateElements_Desc
* mapping and loading CSV file named DEMOGRAPHICS into SqlLite table named demographics
* mapping and loading CSV file named MEASURESOFBIRTHANDDEATH into SQLLite table named birthDate
* create a new query based on demographics and birthDate  into new table named Final
* Update sample fields at Final table by using direct PL/SQL query
* Extract Final data into a CSV file.
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
         "target": ["sqlite", "Finall", -1]},

        {"myexec": ["sqlite", "Update dateElements_Desc Set [Data_Type] = 'dingDong';"]},

        {"source": ["sqlite", "Finall"],
         "target": ["file", "finall.csv"]}
    ]

3. Init class dingDong

:dicObj:        Loading dictionary as a work flow
:dirData:       Loading JSON files in this folder
:includeFiles:  Include files to load from directory folder (dirData)
:notIncldeFiles: Exclude files to load from directory folder (dirData)
:connDict:      Equal to Config.CONN_URL, set connection URLs string
:processes:     Max number of parallel threading to load data (DONG module)

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
* extracting query structure and creating Final table

 Flag -1 - default flag,  indicate that on changed structure- old structure is stored with all data. object is udated to new strucutre

::

    m.ding()

5.  DONG - Extracting and loading data from CSV files into SQLite table, using default truncate-> insert method
    Extract data from a query into Final table

* if object structure changed and mode 1 (default mode)
  * history table will is created
  * new object structure is created. new object is populated with data from history table (only identical column name)

::

        m.dong()

Full sample code::

    import logging
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

We would like to create a platform that will enable to design, implement and maintain data integration project such as:

*  Any REST API connectivity from any API to any API using simple JSON mapping
*  Any Relational database connectivity using JSON mapping
*  Any Non-relational storage
*  Main platform for any middle-ware business logic - from sample if-than-else up to statistics algorithms using ML and DL algorithms
*  Enable Real-time and scheduled integration
*  Single point of truth - maintain all changes by using git source control and enable to compare version and rollback as needed

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

dingDONG was created by `Tal Shany <http://www.biskilled.com>`_
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

