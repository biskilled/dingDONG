.. _dingDong:

Ding DONG data preparation ETL
==============================

Why ding DONG
`````````````

dingDONG is an IT integration tool which was built to enable fast and easy design, implementation and maintain complex data and machine learning projects.
dingDONG support end to end development cycle includes code versioning, maintenance automation and propagation mechanism that
enable developers to maintain, extend and scale complex data projects.

dingDONG give an advantage for machine-learning and deep-learning project by providing simple data preparation layer and experiment version-control layers which
give input/output flexibility and full development life- cycle monitoring.


dingDong main goals are
 - to model (on the fly) diverse SQL and NO-SQL DBS
 - extract and load data optimizing our machine CPU / IO (multithreading, fast connectors )
 - enabling to prepare data easily by adding a new column or manipulating data flow (scaling, normalization..)
 - CI / CD - maintain meta-data versioning, for roll-back and revision compare
 - CI / CD for machine-learning or Deep learning by using embedded GIT functionality
 - enable work-flows flexibility by providing detailed logs and alerts (mail) mechanism
 - extending and improving unique work-flows executers
    - PL/SQL or complex SQL files multiprocess executers
    - OLAP executers
    - REST Apis executers
 - batch or real-time scheduling mechanism

This documentation is the first version of dingDONG, we are open to help from other developers, and we like to receive your comments and feedback.

Please, fill free to contact me at tal@biSkilled.com

We hope to extend dingDONG to be one of the major open-source integration platforms.

**Come and join us!**


..  toctree::
    :maxdepth: 2
    :caption: Installation & Configuration

    ./rst/install


..  toctree::
    :maxdepth: 2
    :caption: MODULES

    ./rst/ding
    ./rst/dong
    ./rst/vc


..  toctree::
    :maxdepth: 2
    :caption: Samples

    ./rst/samples


..  toctree::
    :maxdepth: 2
    :caption: EXECUTORS

    ./rst/sqlScripts
    ./rst/microsoftOLAP
    ./rst/addMessageToSend


..  toctree::
    :caption: About

    ./rst/license


* :ref:`license`