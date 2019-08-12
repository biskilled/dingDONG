.. _tag_mapping:

Ding module
===========

Ding module is used for managing over all meta data structure. propagating any schema changes from source to target.
The goals of this modules are :

* Create or modify target or merge object
  * Create is based on source object data structure
  * Can add or update column name and column type 
* Create supporting query parsing techniques which enable to extract the exact source column types

* Modify existing object managing mechanism

  * Config.TRACK_HISTORY   = True
	* Old structure is copied and stored with all data in it. naming format`:` object_name_[date time stamp]
	* There are three options for managing new object structure, setting is made inside work-flows in TARGET or MERGE objects  
      * -1 DEFAULT,  create new object without any data
      *  1 ADD DATA, add data from old object for identical column names 
      *  2 NO CHANGE, object structure wont update (mostly used for production objects )  	  
	* Any object modification is printed as WARNING into logs 
  * Config.TRACK_HISTORY   = False
    * create or modified object into new structure. if case of modification, data is truncated 


Example of usage
    assuming we are using oracle as source and Sql-server as target :

    - create target strucure based on source query
        query is : "Select field1 As Yoyo, field2 as bobo ... From oracle.table1"
        result



Jason mapping parameters 
########################

sample JSON mapping

Mapping samples
###############


