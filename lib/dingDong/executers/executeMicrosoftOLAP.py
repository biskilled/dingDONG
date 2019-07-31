# (c) 2017-2019, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of dingDong
#
# dingDong is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# dingDong is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDong.  If not, see <http://www.gnu.org/licenses/>.

from dingDong.misc.logger import  p
from dingDong.misc.misc import uniocdeStr


def OLAP_Process(serverName,dbName, cubes=[], dims=[], fullProcess=True):
    import sys, os
    localPath = os.path.abspath(os.path.dirname(__file__))
    sys.path.append(os.path.join(localPath, r'../dll/clrmodule.dll"'))
    import clr
    clr.AddReference(os.path.join(localPath, r'../dll/Microsoft.AnalysisServices.DLL') )

    from Microsoft.AnalysisServices import Server
    from Microsoft.AnalysisServices import ProcessType

    processType = ProcessType.ProcessFull if fullProcess else 0
    # Connect to server
    amoServer = Server()
    amoServer.Connect(serverName)

    # Connect to database
    amoDb = amoServer.Databases[dbName]

    for dim in amoDb.Dimensions:
        if len(dims)==0 or dim in dims:
            try:
                dim.Process(processType)
                p(u"OLAP DB: %s, process DIM %s finish succeffully ... " %(uniocdeStr(dbName, decode=True),uniocdeStr(dim, decode=True)), "i")
            except Exception as e:
                p(u"OLAP DB: %s, ERROR processing DIM %s ... " % (uniocdeStr(dbName, decode=True),uniocdeStr(dim, decode=True)),"e")
                p(e,"e")

    for cube in amoDb.Cubes:
        if len(cubes)==0 or cube in cubes:
            try:
                cube.Process(processType)
                p(u"OLAP DB: %s, CUBE %s finish succeffully ... " %(uniocdeStr(dbName, decode=True),uniocdeStr(cube, decode=True)),"i")
            except Exception as e:
                p(u"OLAP DB: %s, ERROR processing CUBE %s ... " % (uniocdeStr(dbName, decode=True), uniocdeStr(cube, decode=True)),"e")
                p(e,"e")