.. _tag_install:

installation
============

.. |github_link| raw:: html

   <a href="https://github.com/biskilled/dingDong" target="_blank">GitHub</a>

You can download dingDong from |github_link| or by using pip install

.. code-block:: python

    pip install dingDong

.. _tag_config:

configuration
=============
::

    from dingDong import Config


.. _tag_CONN_URL:

Connection url dictionay
------------------------

:Config.CONN_URL:   Connection URL dictionary for setting all connectors connections

* key   -> general connection name or connection type (sql, oracle, file .. )
* value -> can be string or dictionary
  * String     --> Connection string URL (key defined connection type: sql, oracle, mySql....)
  * Dictionary --> must have 'conn' (connection type) and 'url' (connection string).

Available connection can be found at dingDong.misc.enumsJson.eConn::

    Config.CONN_URL    =  {    'sql'    :"DRIVER={SQL Server};SERVER=server,1433;DATABASE=database;UID=uid;PWD=pass;",
                            'oracle' :"DRIVER={SQL Server};SERVER=server,1433;DATABASE=database;UID=uid;PWD=pass;",
                            'mysql'  :"host=host, user=user, passwd=pass, db=db",
                            'vertica':"DRIVER=HPVertica;SERVER=server;DATABASE=database;PORT=5433;UID=user;PWD=pass",
                            'file'   :{'delimiter':',','header':True, 'folder':""}
                       }


:Config.QUERY_PARAMS:   query parameters dictionary for loading parameters into complex queries
::

    Config.QUERY_PARAMS    = {}

:Config.SQL_FOLDER_DIR: SQL folder directory for SQL PL/SQL scripts
::

    Config.SQL_FOLDER_DIR  = None

:Config.PARSER_SQL_MAIN_KEY:   default key for extracting SQL queries from SQL files
::

    Config.PARSER_SQL_MAIN_KEY = "dingDong"

:Config.DECODE:  query decoding
::

    Config.DECODE          = "windows-1255"

:Config.TRACK_HISTORY: Store old object strucure with data when schema changed is detected. format [object_name_YYYYMMDD]
::

    Config.TRACK_HISTORY   = True

:Config.LOOP_ON_ERROR:  Try to insert row by row in a batch loading commit failure
::

    Config.LOOP_ON_ERROR   = True

:Config.NUM_OF_PROCESSES:   Number of parallel threading for dong module (loading and exttracing)
::

    Config.NUM_OF_PROCESSES= 4

:Config.LOGS_<Prop>: Logs files properties
::

    Config.LOGS_DEBUG       = logging.DEBUG     --> set logging level (logging.DEBUG, logging.WARNING...)
    Config.LOGS_DIR         = None              --> if Directory is set, logs files will be created
    Config.LOGS_INFO_NAME   = 'log'             --> default info log    : log.info
    Config.LOGS_ERR_NAME    = 'log'             --> default error log   : log.err
    Config.LOGS_TMP_NAME    = 'lastLog'         --> store last execution log lastLog.err and lastLog.warning, used for send logs at the end of the work flow
    Config.LOGS_HISTORY_DAYS= 5                 --> will delete files older than 5 days

:Config.SMTP_<Prop>:    SMTP configuration for sending workflow massages (ERROR, SUCCESS, WARNING)
::

    Config.SMTP_SERVER             = ''
    Config.SMTP_SERVER_USER        = ''
    Config.SMTP_SERVER_PASS        = ''
    Config.SMTP_SENDER             = ''
    Config.SMTP_RECEIVERS          = ['info@biSkilled.com']