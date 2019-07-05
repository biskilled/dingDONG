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

import copy
from collections import OrderedDict

from dingDong.bl.baseBL import baseBL
from dingDong.misc.logger import p

class model(baseBL):
    def __init__ (self, **args):
        baseBL.__init__(self, name='Model loading', **args)

    def execJsonNode (self, jsName, jsonNodes):
        mergeSource = None
        for jMap in jsonNodes:
            self.setSTT(node=jMap)
            sourceStt = None
            targetStt = None
            for node in jMap:
                if isinstance(jMap[node], (list, tuple)):
                    self.execJsonNode(jsName=jsName, jsonNodes=jMap[node])
                    continue

                if not isinstance(jMap[node], (dict, OrderedDict) ):
                    p("Not valid json values - must be list of dictionary.. continue. val: %s "%(str(jMap)),"e")
                    continue

                self.setMainProperty(key=node, valDict=jMap[node])

                if self.src:
                    mergeSource = copy.copy (self.src)

                if self.tar and self.src:
                    # convert source data type to target data types
                    targetStt = self.updateTargetBySourceAndStt(src=self.src, tar=self.tar)

                    self.tar.create (stt=targetStt)
                    mergeSource = copy.copy (self.tar)
                    self.src.close()
                    self.tar.close()

                    sourceStt = None
                    self.src  = None
                    self.tar  = None

                elif self.tar and self.stt:
                    self.tar.create(stt=self.stt)
                    mergeSource = copy.copy (self.tar)
                    self.tar.close()
                    self.tar = None

                if self.mrg and mergeSource:
                    sttMerge = self.updateTargetBySourceAndStt(src=mergeSource, tar=self.mrg)
                    self.mrg.create(stt=sttMerge)
                    self.mrg.close()
                    self.mrg = None