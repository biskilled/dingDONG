.. _tag_install:

installation
============

.. |github_link| raw:: html

   <a href="https://github.com/biskilled/dingDong" target="_blank">GitHub</a>

You can dowload dingDong from |github_link| or by usin pip install::

    pip install popEtl

.. code-block:: python

    for i in range(10):
        print(i)


.. _tag_config:

configuration
=============
::

    from dingDong import Config



.. _tag_CONN_URL:

Connection url dictionay
------------------------
:CONN_URL: Connection URL dictionary for setting connectors connections

* Config.CONN_URL   -> set connection URL into all connectors
  * key   -> general connection name or connection type (sql, oracle, file .. )
  * value -> can be string or dictionary
    * String     --> Connection string URL (key defined connection type: sql, oracle, mySql....)
    * Dictionary --> must have 'conn' (connection type) and 'url' (connection string).

Available connection can be found at dingDong.misc.enumsJson.eConn

::

    CONN_URL    =  {    'sql'    :"DRIVER={SQL Server};SERVER=server,1433;DATABASE=database;UID=uid;PWD=pass;",
                            'oracle' :"DRIVER={SQL Server};SERVER=server,1433;DATABASE=database;UID=uid;PWD=pass;",
                            'mysql'  :"host=host, user=user, passwd=pass, db=db",
                            'vertica':"DRIVER=HPVertica;SERVER=server;DATABASE=database;PORT=5433;UID=user;PWD=pass",
                            'file'   :{'delimiter':',','header':True, 'folder':""}
                       }


2. query parameters dictionary for loading parameters into complex queries ::

    QUERY_PARAMS    = {}

3. SQL folder directory for SQL PL/SQL scripts ::

    SQL_FOLDER_DIR  = None

4. default key for extracting SQL queries from SQL files ::

    PARSER_SQL_MAIN_KEY = "dingDong"

5. query decoding ::

    DECODE          = "windows-1255"

6. enable to store object with data if schema is changed.
   old schema stored as [object_name_YYYYMMDD] name format ::

    TRACK_HISTORY   = True

7. Extract and load use batch size commit. set LOOP_ON_ERROR to true will insert row by row on case of batch commit failure

::

    LOOP_ON_ERROR   = True

8. Number of parallel threading to execute in loading module::

    NUM_OF_PROCESSES= 4

9. Logs files properties::

    LOGS_DEBUG = logging.DEBUG          --> set logging level (logging.DEBUG, logging.WARNING...)
    LOGS_DIR   = None                   --> if Directory is set, logs files will be created
    LOGS_INFO_NAME = 'log'              --> default info log    : log.info
    LOGS_ERR_NAME  = 'log'              --> default error log   : log.err
    LOGS_TMP_NAME  = 'lastLog'          --> store last execution log lastLog.err and lastLog.warning, used for send logs at the end of the work flow
    LOGS_HISTORY_DAYS=5                 --> will delete files older than 5 days

10. SMTP configuration for send success, failed or warning massages::

    SMTP_SERVER             = ''
    SMTP_SERVER_USER        = ''
    SMTP_SERVER_PASS        = ''
    SMTP_SENDER             = ''
    SMTP_RECEIVERS          = ['info@biSkilled.com']

