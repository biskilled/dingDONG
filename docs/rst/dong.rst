.. _tag_dong:

Dong module
===========

Dong module is used to extract and load massive data volumne from one connection to another. there are methods to add function on a specific column
or adding new column with calculated function based on other column at the source connection. 
Dong support multi-threading mechanism which allow to execute several work-flow at once. 

The goals of this module:

* extract data from source objects
* data cleansing by using calculated function on columns 
* adding calculated columns 
* fast loading into diverse connection types 

Main Configuration properties:

* Config.SQL_FOLDER_DIR -> executing queries/ PL SQL function from SQL file located at SQL_FOLDER_DIR 
* Config.LOOP_ON_ERROR  -> will load row by row if batch insert failed
* Config.NUM_OF_PROCESSES-> maximum threading that will run in parallel 
* Config.LOGS_DEBUG		-> set logging level (ERROR, WARNING, DEBUG, INFO )
* Config.LOGS_DIR		-> log files folder 


Json loading params
###################

Sample json loading

Loading samples
###############

sample loading

