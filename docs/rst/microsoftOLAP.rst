.. _tag_olap:

Executor: microsoft OLAP
========================

Executing Microsoft OLAP cubes and dimensions, using Microsoft.AnalysisServices.DLL.

::
    import logging
    from dingDong import DingDong
    from dingDong import Config

    Config.LOGS_DEBUG = logging.DEBUG

    dd = DingDong()
    dd.execMicrosoftOLAP(serverName, dbName, cubes=[], dims=[], fullProcess=True)

:serverNsme:    Microsoft analysis services instance name
:dbName:        Analysis services database name
:cubes:         processing cubes
  * cubes = []: processing all existing cubes
  * cubes = ['cube1', 'cube2',...]: process cube1, cube2 ..
:dims:          processing dimensions
  * dims = []: processing all shared dimensions
  * dims = ['dimension_1', 'dimension_2', 'dimension_3', ...]: process only dimension_1, dimension_2, dimension_3
:fullProcess:   analysis process type  - FULL or PARTIAL
