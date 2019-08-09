|PyPI version| |Docs badge| |License|

*********
Ding Dong
*********

dingDong created for modeling devlop and maintin complex data integration projects - relational database
or cloud APIs, Sql or no sql, data cleansing or modeling algorithms.

The project is currently support and batch loading, our next phase is to extend it for REST and websockecet
APIs itegration as well.

Current project is purely python based. REST and websocket will be implemnted by nodeJs.

This project aims to use as a glue between diverse data storage types.
we did not implementeed any JOIN or UNION method which can be used in much efficient way at the connectoer themself
we did impleented pure meta data manager, data tranformation and extracting method.
Using the capabiltes of existing connectors with dingDong allow us to create robust data project using the
adavatges of all the componenents

Connectors :
        Sql server  - tested, ready for production
        Oracle      - tested, ready for production
        SqlLite     - tested, ready for production
        text files  - tested, ready for production
        CSV         - tested
        Vertica     - partially tested
        MySql       - partially tested
        MongoDB     - partially tested
        Hadoop/Hive - not implemted

        API Support
        SalesForce  - partially, not tested


dingDong is splitted to two main moduls:
 - DING - create and manage overall metadata strucutre for all object at the workflow
         - creating new object
         - modify existing object by using back propogation mechnism
         - update data into now object
         - store old strucure
         - (todo) --> truck all changes in main repo for CI/CD processess

- DONG - extract and load data from diverse realtion / not relational connectors
    - extract data - support multithreading for extracting massive data volume
    - transfer     - enable to manipylate date by adding manipulation function on column
                   - enable to add custom calculated fields
    - merge        - merging source with target data can be done if source and merge located at the same connector
    - exec         - enable to execute PL/SQL or sstored procedure command as part of the whole data workflow

Read more about dingDong at http://www.biSkilled.com (marketing) or at `dingDong documentation <https://readthedocs.org/projects/popeye-etl/>`_

Installation
============
`download from GitHub <https://github.com/biskilled/dingDong>`_ or install by using ``pip``

``
pip install dingDong
``


Samples
=======
download samples file

1. :download:`DATAELEMENTDESCRIPTION.csv <samples/sampleHealthCare/csvData/DATAELEMENTDESCRIPTION.csv>`.

2. .. image:: samples/sampleHealthCare/csvData/DATAELEMENTDESCRIPTION.csv

3. `DATAELEMENTDESCRIPTION.csv <samples/sampleHealthCare/csvData/DATAELEMENTDESCRIPTION.csv>`_

4. .. |location_link| raw:: html
    <a href="http://www.ynet.co.il" target="_blank">check your location here</a>




:download:`DEMOGRAPHICS.csv <https://github.com/biskilled/dingDong/blob/master/samples/sampleHealthCare/csvData/DATAELEMENTDESCRIPTION.csv>`_
`MEASURESOFBIRTHANDDEATH.csv <https://github.com/biskilled/dingDong/samples/sampleHealthCare/csvData/MEASURESOFBIRTHANDDEATH.csv>`_

in this sampel we will use `C:\samples` as root directory for all files and logs

.. code-block::python
def xxxx(x):
    print ("HALLL")

::
    Hallo world hoe
    def xxxx ():
        print ("GAlllo")


hallow world
dsdsdsdsd ``::``


Road map
========

We would like to create a platform that will enable to design, implement and maintenance and data integration project such as:

*  Any REST API connectivity from any API to any API using simple JSON mapping
*  Any Relational data base connectivity using JSON mapping
*  Any Non relational storage
*  Main platform for any middleware business logic - from sample if-than-else up to statistics algorithms using ML and DL algorithms
*  Enable Real time and scheduled integration

We will extend our connectors and Meta-data manager accordingly.

Cuurent supporting features
===========================

*  APIs       : Salesforce
*  RMDBs      : Sql-Server, Access, Oracle, Vertice, MySql
*  middleware : column transformation and simple data cleansing
*  DBs        : mongoDb
*  Batch      : Using external scheduler currently .....
*  onLine     : Needs to be implemented .....

Authors
=======

dingDong was created by `Tal Shany <http://www.biskilled.com>`_
(tal@biSkilled.com)
We are looking for contributions !!!

License
=======

GNU General Public License v3.0

See `COPYING <COPYING>`_ to see the full text.

.. |PyPI version| image:: https://img.shields.io/pypi/v/dingDong.svg
   :target: https://github.com/biskilled/dingDong
.. |Docs badge| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :target: https://readthedocs.org/projects/dingDong/
.. |License| image:: https://img.shields.io/badge/license-GPL%20v3.0-brightgreen.svg
   :target: COPYING
   :alt: Repository License
   