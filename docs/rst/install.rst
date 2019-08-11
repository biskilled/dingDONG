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


1. Connection URL dictionay for setting connectors connections ::

    CONN_URL    =  {    'sql'    :"DRIVER={SQL Server};SERVER=server,1433;DATABASE=database;UID=uid;PWD=pass;",
                            'oracle' :"DRIVER={SQL Server};SERVER=server,1433;DATABASE=database;UID=uid;PWD=pass;",
                            'mysql'  :"host=host, user=user, passwd=pass, db=db",
                            'vertica':"DRIVER=HPVertica;SERVER=server;DATABASE=database;PORT=5433;UID=user;PWD=pass",
                            'file'   :{'delimiter':',','header':True, 'folder':""}
                       }

2. query paramters dicionary for loading paramters into complex queries ::

    QUERY_PARAMS    = {}

3. Sql folder directory for SQL PL/SQL scripts ::

    SQL_FOLDER_DIR  = None

4. default key for extracting SQL queries from SQL files ::

    PARSER_SQL_MAIN_KEY = "dingDong"

5. query decoding ::

    DECODE          = "windows-1255"

6. enable to store object with data if schema is changed.
   old schema stored as [object_name_YYYYMMDD] name format ::

    TRACK_HISTORY   = True

7. Extract and load use batch size commit. in case of commit failure - there is an option to commint single line
   to maximise loading success ::

    LOOP_ON_ERROR   = True

8. Number of parrallel threding to execute in loading module::

    NUM_OF_PROCESSES= 4

9. Logs files properties::

    LOGS_DEBUG = logging.DEBUG          --> Logging level
    LOGS_DIR   = None                   --> if Direcory is set, logs files will be created
    LOGS_INFO_NAME = 'log'              --> default info log    : log.info
    LOGS_ERR_NAME  = 'log'              --> default error log   : log.err
    LOGS_TMP_NAME  = 'lastLog'          --> store last execution log lastLog.err and lastLog.warning, used for send logs at the end of the workflow
    LOGS_HISTORY_DAYS=5                 --> will delete files older than 5 days

10. SMTP configuration for send success/ failed/ warning massages::

    SMTP_SERVER             = ""
    SMTP_SERVER_USER        = ""
    SMTP_SERVER_PASS        = ""
    SMTP_SENDER             = ""
    SMTP_RECEIVERS          = ['info@biSkilled.com']

