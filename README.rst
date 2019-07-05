|PyPI version| |Docs badge| |License|

******
popEtl
******

popEtl created for modeling and implementing any data integration project- if it is small or large amount of data,
relational database or cloud APIs, Sql or no sql, data cleansing or modeling algorithms - popEtl aims to give a
single platform that can model any data strucure, load data from one source to another and be able to maintain all
required changes that any integration project deal in fast and clean manner
The project started to be build becouse we are lazy and we like automation any we hate maintenance !!

Read more about popEtl at http://www.biSkilled.com (marketing) or at `popEtl documentation <https://readthedocs.org/projects/popeye-etl/>`_

Installation
============
`download from GitHub <https://github.com/biskilled/popEtl>`_ or install by using ``pip``


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

popEtl was created by `Tal Shany <http://www.biskilled.com>`_
(tal@biSkilled.com)
We are looking for contributions !!!

License
=======

GNU General Public License v3.0

See `COPYING <COPYING>`_ to see the full text.

.. |PyPI version| image:: https://img.shields.io/pypi/v/popEtl.svg
   :target: https://github.com/biskilled/popEtl
.. |Docs badge| image:: https://img.shields.io/badge/docs-latest-brightgreen.svg
   :target: https://readthedocs.org/projects/popeye-etl/
.. |License| image:: https://img.shields.io/badge/license-GPL%20v3.0-brightgreen.svg
   :target: COPYING
   :alt: Repository License
   