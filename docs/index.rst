.. _dingDong:

dingDong Documentation
======================

About dingDong
``````````````

dingDong is an IT integration tool which was build to enable fast and fun design, implement and maiantane any data project.
To do that we developed integration and modeling blockes that enable to model diverse SQL DBs
and No SQL DB, extract data from one source to another and maintain business logic in and easy SQL/python format.

dingDong's main goals are creating and mainting data CI/CD workflow in a fast integration cycles and enabling to implement new requirement in a fast manner. It also provide a platform that enable to test and update business logic using the best of SQL in one hand and python on the other.

We believe that integration can be fast and much more simple buy using simple scripting language buy implementing 2 major concept :

* Fast design - the mapping module enable to create / maintance data model from scratch or by using existing data model.
  For example : to create full DWH implementation in Vertica based on mongo - all we have to do is create DWH model (entities, fields) using simple JSON format.
  dingDong will manage all data type strucure and internal Vertica objects based on Mongo structure
* Fast Extract and loading - Loader module enable to full load / merge / increment methods for loading data on a scheduled process based on JSN defined mapping structure

This documentations are first version of dingDong, we do look for your help and we will provide our "wish-list" .

dingDong hope to extend and be one of the major open-source integration platform. come and join us

Quick sample:
- loading CSV file into temp DB (SqlLite), merging and extracting results into another DB and CSV file

..  toctree::
    :maxdepth: 2
    :caption: Installation & Configuration

    ./rst/install

..  toctree::
    :maxdepth: 2
    :caption: MODULES

    ./rst/ding
    ./rst/dong

..  toctree::
    :maxdepth: 2
    :caption: EXECUTERS

    ./rst/sqlScripts
    ./rst/microsoftOLAP
    ./rst/addMassageToSend

..  toctree::
    :maxdepth: 2
    :caption: Samples

    ./rst/samples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
* :ref:`license`
