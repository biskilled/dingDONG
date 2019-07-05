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

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from collections import OrderedDict

from dingDong.bl.baseBL           import baseBL
from dingDong.misc.logger         import p

class loader(baseBL):
    def __init__ (self, **args):
        baseBL.__init__(self, name='Model loading', **args)

    def execJsonNode (self, jsName, jsonNodes):
        self.src            = None
        self.tar            = None
        self.mrg            = None
        self.exec           = None
        self.stt            = None
        self.addSourceColumn= True

        for jMap in jsonNodes:
            self.setSTT(node=jMap)

            for node in jMap:
                if isinstance(jMap[node], (list, tuple)):
                    self.execJsonNode(jsName=jsName, jsonNodes=jMap[node])
                    continue

                if not isinstance(jMap[node], (dict, OrderedDict)):
                    p("Not valid json values - must be list of dictionary.. continue. val: %s " % (str(jMap)), "e")
                    continue

                self.setMainProperty(key=node, valDict=jMap[node])


                if self.exec:
                    """ Execute internal connection procedure """
                    self.exec = None

                if self.src and self.tar:
                    """ TRANSFER DATA FROM SOURCE TO TARGET """
                    self.tar.preLoading()

                    tarToSrc = self.mappingLoadingSourceToTarget (src=self.src, tar=self.tar)
                    self.src.extract(tar=self.tar, tarToSrc=tarToSrc, batchRows=10000, addAsTaret=True)
                    self.tar.close()
                    self.src.close()

                    self.src = None
                    self.tar = None

                """ MERGE DATA BETWEEN SOURCE AND TARGET TABLES """
                if self.mrg and self.mergeSource:
                    self.mrg.merge (mergeTable=self.mergeTarget, mergeKeys=self.mergeKeys, sourceTable=None)
                    self.mrg.close()

                    self.mergeSource= None
                    self.mergeTarget= None
                    self.mergeKeys  = None