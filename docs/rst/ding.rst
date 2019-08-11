.. _tag_mapping:

Ding module
===========

Ding module is used for managing over all meta data strucure. propogating changes from source to target and track
chages. The goals of this modules are :

- create target or merge object if not exists.
    create is bsed on source object data strucure
    create can add additonal columns or update column types
- supporting query parsing technices to extract direct column types from source connection
- On changes, there are 3 option to track history :
    1.  store history and create new strucutre object
        old strucure will be coped into new object named [object_timestemp] with all exisitng data
        new strucure will be created
    2.  store history and create new strucure object
        old strucure object will be clone into new object named [object_timestemp] with all exisitng data
        new strucure object will be created
        inserting data from old strucure to new strucure for similar column names
    3.  new strucutre discovered - but object is not allowed to change
        warning will be printed into LOG files
        This scenario is used moslty for DWH table which must be used in production mode




Creating new target or merge object based on source definitioins.

and anaging meta data strucure

Ding module is used to create target structure based on source systems strucute or by define strucure
in a JSON format.

Example of usage
    assuming we are using oracle as source and Sql-server as target :

    - create target strucure based on source query
        query is : "Select field1 As Yoyo, field2 as bobo ... From oracle.table1"
        result



Jason mapping params
####################

sample json mapping

Mapping samples
###############

sample mapping

