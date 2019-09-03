.. _dingDong:

dingDong Documentation
======================

About dingDong
``````````````

dingDong is an IT integration tool which was built to enable fast and easy design, implementation and maintained any data project.
To do that we developed integration and modeling blocks that enable to model diverse SQL DBs
and No SQL DB, extract data from one source to another and maintain business logic in an easy SQL/python format.

dingDong's main goals are creating and maintaining CI/CD data workflows in a fast integration cycle and enabling to implement the new requirement in a fast manner. It also provides a platform that enable to test and update business logic using the best of SQL in one hand and python on the other.

We believe that integration can be fast and much more simple buy using simple scripting language buy implementing two major concepts:

* Fast design - the mapping module enables to create / maintenance data model from scratch or by using an existing data model.
  For example: to create full DWH implementation in Vertica based on mongo - all we have to do is create a DWH model (entities, fields) using a simple JSON format.
  dingDong will manage all data type structure and internal Vertica objects based on Mongo structure
* Fast Extract and loading - Loader module enable to full load/merge/increment methods for loading data on a scheduled process based on JSN defined mapping structure

This documentation is the first version of dingDong, we do look for your help and we will provide our "wish-list" .

we hope to extend our development and be one of the major open-source integration platforms. come and join us


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
    :caption: Samples

    ./rst/samples

..  toctree::
    :maxdepth: 2
    :caption: EXECUTERS

    ./rst/sqlScripts
    ./rst/microsoftOLAP
    ./rst/addMessageToSend

..  toctree::
    :caption: About

    ./rst/license

* :ref:`license`