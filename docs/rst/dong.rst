.. _tag_dong:

Dong module
===========

Dong module is used to extract and load massive data volume. there are built-in functions
that can be used for column manipulation, It is possible to add functionality for any data manipulations.
the function can use one of more column as source function input. the output will be a target column.
Dong support multi-threading mechanism which allows executing several work-flow at a time.

Dong support two main loading methods:

- **Truncate-> Insert:** Loading data to an empty object
- **delete-> Insert:** Loading incremental data into an object


The goals of this module:

* extract and load data
* data cleansing by using the calculated function on  columns
* Ability to add calculated column
* fast massive loading into diverse connection types

Main Configuration properties:

* Config.SQL_FOLDER_DIR -> executing queries/ PL SQL function from SQL file located at SQL_FOLDER_DIR
* Config.LOOP_ON_ERROR  -> will load row by row if batch insert failed
* Config.NUM_OF_PROCESSES-> maximum threading that will run in parallel
* Config.LOGS_DEBUG		-> set logging level (ERROR, WARNING, DEBUG, INFO )
* Config.LOGS_DIR		-> log files folder

JSON work-flow
##############

All work-flows takes using :ref:`tag_CONN_URL`  (``Config.CONN_URL`` in the config file) as a source for all connection properties.
with merging connection properties and JSON vales there are all available properties for each work-flow node (connection URL object, pre-execution, post-execution, etc)

full JSON keys can be found at enum config file located at `GitHub <https://github.com/biskilled/dingDong/blob/master/lib/dingDong/misc/enumsJson.py>`_
listed below available JSON key and values

- **TARGET**: destination connection. target connection to load data into. defult pre loading method - truncate data. pre ;oading change to delete if there is a filter clouse. keys: ``t``, ``target``. possible values:

 - **object name:** search for ``target`` or ``t`` as connection name, set object name. truncate data before starting to load
 - **[connection name, object name]:** set connection from connection properties, set object name. truncate data before starting to load
 - **[connection name, object name, delete where clouse]:** set connection from connection properties, set object name, **delete data** from target object before starting to load
 - **{'conn':'connection name', 'obj':'object name','filter':'delete where clouse..'}:** Using a dictionay to set all target loading properties

- **SOURCE**: source object that used to extract data from. adding filter will load only filtered data by adding **WHERE** clause. keys: ``s``, ``source``. possible values:

 - **object name:** search for ``source`` or ``s`` as connection name, set object name.  truncate data before starting to load
 - **[connection name, object name]:** set connection from connection properties, set object name.  truncate data before starting to load
 - **[connection name, object name, filter where clouse]:** set connection from connection properties, set object name, **filter data** from the source object

- **QUERY**: source query to extract data from divers objects.  keys: ``q``, ``query``. possible values:

 - **query:** search for ``query`` or ``q`` as connection name, set query as object to load
 - **[connection name, query]:** set connection from connection properties, set query as object name

- **MERGE**: merge object with target or source (if target object not exists) at the same connection. merge need to have identical columns names as column keys and identical column names as update columns. keys: ``m``, ``merge``. possible values:

 - **merge object name:** search for ``merge`` or ``m`` as connection name, set source to merge from target node (or source if target not exists). merge will use all identical column names as column key
 - **[merge object name, [-1,1,2]]:** set connection from connection properties, set target merge from merge object name, set merge :ref:`tag_schema_modify
 - **[merge object name, list merge keys]:** set connection from connection properties, set target to merge from the merge object name, set merge column from list merge keys. all remaining identical column will be updated

- **Map source to target-set**: a dictionary to map target to data transformation functions and adding calculated columns. sample value: ``{'target column name':{'type':XXX, 'source':YYY, 'f':'fDate()', ..} ...}``. key: value for Dong listed below
- **Map source to target-sttappend**: this is used to add column to all existing column in the source object. if the column exists than sttappend update properties accordingly.  key: value for Dong listed below

 - **source column name: s:** ``{'s':'Source column name'}`` source column name as inout for tranforming data
 - **function: f:** ``{'f':'function_name()'}`` set fuction method to transform data. new function can be added (samples below)
  - full available functions can be found under `function list in github <https://github.com/biskilled/dingDong/blob/master/lib/dingDong/conn/baseBatchFunction.py>`_  existing function:
   - **fDCast()**: ``{'f':'fDcast()'}``: Convert: None-> current; not valid data->None; 'YYYYMMDD'->'MM/DD/YYYY'
   - **fDTCast**: ``{'f':'fDTCast'}``: Convert:  input: 'YYYYMMDD' or 'YYYYMMDDHHmmSS',    output: MM/DD/YYYY. if value is None return current date. if value not valid retunr None


 - **execution function: e:** ``{'e':'{column1}{column2}{column3}'}``set execution method to use multiple source column as input function using input as source and output as target. more details and sample can be found at dong module


.. _tag_functions:

Extract functions
#################

function class can be found in `github <https://github.com/biskilled/dingDong/blob/master/lib/dingDong/conn/baseBatchFunction.py>`_
and can be added by inherited fncBase class

Built in functions:

:fDCast:    Date string format convert. `YYYYMMDD` to `mm/dd/yyyy` format. None - if string not valid
:fDTCast:   DateTime string format convert. `YYYYMMDDHHMMSS` to  `mm/dd/yyyy hh/mm/ss` format. None - if string not valid
:fDFile:    Date string format convert. `dd/mm/yy` to  `mm/dd/yyyy` format. None - if string not valid
:fDCurr:    Return current system dataTime
:fTCast:    Time string format convert. `HHMMSS` to  `HH:MM:SS` format. None - if string not valid
:fR:        Replace column string with another string. `fR(searchString, newString)`
:fNull:     Return default value if column is None. `fNull(defaultValue)`
:fClob:     return None if string is empty
:fDecode:   Convert unicode string to STR
:fPhone:    Not fully implemented: phone validation
:fAddress:  Not fully implemented: address validation




