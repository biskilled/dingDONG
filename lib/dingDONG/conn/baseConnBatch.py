# (c) 2017-2021, Tal Shany <tal.shany@biSkilled.com>
#
# This file is part of dingDONG
#
# dingDONG is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any late r version.
#
# dingDONG is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with dingDONG.  If not, see <http://www.gnu.org/licenses/>.


import abc
import six

from dingDONG.conn.baseConn import baseConn
from dingDONG.misc.enums    import eConn
from dingDONG.misc.globalMethods import setProperty, findEnum
from dingDONG.misc.logger   import p

@six.add_metaclass(abc.ABCMeta)
class baseConnBatch ( baseConn ):
    def __init__ (self,propertyDict=None, **args):
        """ ABSTRACT METHOD THAT MUST BE IMPLEMETED IN ANY CHILD CLASSES """

        baseConn.__init__(self, propertyDict=propertyDict, **args)
        self.dataTypes = self.setDataTypes(connDataTypes=setProperty(k= eConn.props.DATA_TYPES, o=args, defVal={}))

        self.connType = setProperty(k=eConn.props.TYPE, o=self.propertyDict)
        self.connName = setProperty(k=eConn.props.NAME, o=self.propertyDict, defVal=self.connType)
        self.connIsSrc = setProperty(k=eConn.props.IS_SOURCE, o=self.propertyDict, defVal=False)
        self.connIsTar = setProperty(k=eConn.props.IS_TARGET, o=self.propertyDict, defVal=False)
        self.connIsSql = setProperty(k=eConn.props.IS_SQL, o=self.propertyDict, defVal=False)
        self.connFilter = setProperty(k=eConn.props.FILTER, o=self.propertyDict, defVal=None)
        self.connUpdateMethod = setProperty(k=eConn.props.UPDATE, o=self.propertyDict)

        self.isSingleObject = True


        if not findEnum(prop=self.connType, obj=eConn.types):
            err = "{}: Connection NOT VALID !!".format(self.connType)
            raise ValueError(err)

    """ ABSTRACT METHOD THAT MUST BE IMPLEMETED IN ANY CHILD CLASSES """

    @abc.abstractmethod
    def connect(self):
        raise NotImplemented("baseConn-> Connect Method is not implemented !!!")

    @abc.abstractmethod
    def close(self):
        pass

    def test(self):
        if not  self.connect():
            p("CONNECTING FAILED: %s, type: %s " % (self.connName, self.connType), "e")
        return True

    @abc.abstractmethod
    def isExists(self, **args):
        pass

    @abc.abstractmethod
    def create(self, sttDict=None, addIndex=None):
        pass

    @abc.abstractmethod
    def getStructure(self, objects=None,  **args):
        pass

    @abc.abstractmethod
    def preLoading(self, dictObj=None):
        pass

    @abc.abstractmethod
    def extract(self, tar, tarToSrcDict, **args):
        pass

    @abc.abstractmethod
    def load(self, **args):
        pass

    @abc.abstractmethod
    def execMethod(self, method=None):
        pass

    @abc.abstractmethod
    def merge(self, mergeTable, mergeKeys=None, sourceTable=None, ignoreUpdateColumn=None):
        pass

    @abc.abstractmethod
    def cntRows(self, objName=None):
        pass

    @abc.abstractmethod
    def createFromDbStrucure(self, stt=None, objName=None, addIndex=None):
        pass