.. _tag_ding:

Ding module
===========

Ding module is used for managing overall metadata structure. any schema change in source object will be propagated to all target or merge object as well.
It is possible to create different workflows which maintain different data storage and will be maintained in one single point.

Ding used for:

* Creating new schema or modify existing for target or merge objects
* Schema structure can be based on source meta-data object or on  query structure
* Support adding a column and updating column types
* Support source to target column mapping
* Support adding indexes
* Schema modifications are documented at logs or in the internal repo for history track changes

.. _tag_schema_modify:

Schema modification policy
--------------------------
There are three option to set if object is exists and modified:

:-1 DEFAULT: The old structure is copied and stored with all data in it. naming format: ``object name_[YYYYMMDD]``. New schema structure is created with no data
:1 ADD DATA: The old structure is copied and stored with all data in it. naming format: ``object name_[YYYYMMDD]``. New schema structure is created with data in columns with the same name as an old object structure
:2 NO CHANGE: object structure cannot be modified

JSON work-flow
##############

All work-flows takes using :ref:`tag_CONN_URL`  (``Config.CONN_URL`` in config file) as a source for all connection properties.
with merging connection properties and JSON vales there is all availabe properties for each work-flow node (connection url object, pre exection, post execution etc)

full JSON keys can be found at enum config file located at `github <https://github.com/biskilled/dingDong/blob/master/lib/dingDong/misc/enumsJson.py>`_
listed below availabe JSON key and values

- **TARGET**: destination connection. data structure depends on source object. strucuture can be modified by using ``columns``, ``mapping``, ``stt`` or ``sttappend`` nodes. available key strings: ``t``, ``target``. possible values:

 - **object name:** search for ``target`` or ``t`` as connection name, set object name
 - **[connection name, object name]:** set connection from connection properties, set object name
 - **[connection name, object name, delete where clouse]:** set connection from connection properties, set object name, execute pre loading *delete data* from target (Dong module)
 - **[connection name, object name,  [-1,1,2] ]:** set connection from connection properties, set object name, set :ref:`tag_schema_modify`
 - **[connection name, object name, delete where clouse, [-1,1,2] ]:** set connection from connection properties, set object name, execute pre loading *delete data* from target (Dong module), set :ref:`tag_schema_modify`
 - **{'conn':'connection name', 'obj':'object name','filter':'delete where clouse..','update':[-1,1,2]....}:** dictionay to set all properties. avaialbe propertis keys can be found under jValues at `github <https://github.com/biskilled/dingDong/blob/master/lib/dingDong/misc/enumsJson.py>`_

- **SOURCE**: source object that used to extract data from. available key strings: ``s``, ``source``. possible values:

 - **object name:** search for ``source`` or ``s`` as connection name, set object name
 - **[connection name, object name]:** set connection from connection properties, set object name
 - **[connection name, object name, filter where clouse]:** set connection from connection properties, set object name, execute pre loading *filter data* from source (Dong module)

- **QUERY**: Query that used to extract data from. available key strings: ``q``, ``query``. possible values:

 - **query:** search for ``query`` or ``q`` as connection name, set query as object to load
 - **[connection name, query]:** set connection from connection properties, set query as object name

- **MERGE**: merge object with target or source (if target object not exists) at the same connection. merge need to have identical columns names as column keys and identical column names as update columns. available key strings: ``m``, ``merge``. possible values:

 - **merge object name:** search for ``merge`` or ``m`` as connection name, set source merge from target node (or source if target not exists). merge will use all identical column names as column key
 - **[merge object name, [-1,1,2]]:** set connection from connection properties, set target merge from merge object name, set merge :ref:`tag_schema_modify
 - **[merge object name, list merge keys]:** set connection from connection properties, set target merge from merge object name, set merge column from list merge keys. all remaining identical column will be updated
 - **[merge object name, list merge keys, [-1,1,2]]:** set connection from connection properties, set target merge from merge object name, set merge column from list merge keys. all remaining identical column will be updated, set merge :ref:`tag_schema_modify

- **Map source to target-STT**: dictionary to define target column schema properties. using STT will define all target object strucure. available key strings: ``stt``, ``sourcetotarget``. value is a dictinary - {'target column name':{'type':XXX, 'source':YYY ...} ...} possible properties for schema defintion:

 - **source column name: s:** ``s`` is a key for using source column type for target column and mapping source to target for extracting and loading data (dong module)
 - **data type: t:** ``t`` is key to map target column data type
 - **aliase name: a:** ``a`` is using to use alias name as target column name
 - **function: f:** set fuction using inout as source and output as target. more details and sample can be found at dong module
 - **execution function: e:** set excecution method to use multiple source column as input fuction using inout as source and output as target. more details and sample can be found at dong module

Mapping samples
###############

 .. code-block:: python

    Config.CONN_URL = {'target':{'conn':'sql', url:"DRIVER={SQL Server};SERVER=server,1433;DATABASE=database;UID=uid;PWD=pass;"}}

    # option 1
    work-Flow = [{ "target":"targetTableName" }]

    # option 2
    work-Flow = [{ "target":["sql","targetTableName"] }]




