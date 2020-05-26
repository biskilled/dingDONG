
from collections import OrderedDict

from dingDONG.misc.enums import eConn
from dingDONG.conn.connDB       import connDb
from dingDONG.conn.connAccess   import access
from dingDONG.conn.connMongo    import connMongo
from dingDONG.conn.connFile     import connFile
from dingDONG.misc.logger       import p
from dingDONG.config            import config

CLASS_TO_LOAD = {eConn.types.SQLSERVER :connDb,
                 eConn.types.ORACLE:connDb,
                 eConn.types.VERTICA:connDb,
                 eConn.types.ACCESS:access,
                 eConn.types.MYSQL:connDb,
                 eConn.types.LITE:connDb,
                 eConn.types.FILE:connFile,
                 eConn.types.FOLDER:connFile,
                 eConn.types.MONGO:connMongo,
                 eConn.types.POSTGESQL:connDb}


def addPropToDict (existsDict, newProp):
    if newProp and isinstance(newProp, (dict, OrderedDict)):
        for k in newProp:
            if k in existsDict and isinstance(newProp[k], dict):
                existsDict = addPropToDict (existsDict, newProp=newProp[k])
            elif k not in existsDict:
                existsDict[k] = newProp[k]
            elif k in existsDict and existsDict[k] is None:
                existsDict[k] = newProp[k]
    elif isinstance(newProp,str):
        existsDict[eConn.props.URL] = newProp
    else:
        p("THERE IS AN ERROR ADDING %s INTO DICTIONARY " %(newProp), "e")

    return existsDict

def mngConnectors(propertyDict, connLoadProp=None):

    connLoadProp = connLoadProp if connLoadProp else config.CONNECTIONS

    ## MERGE propertyDict with values from CONNECTION by connType
    if eConn.props.TYPE in propertyDict and propertyDict[eConn.props.TYPE] in connLoadProp:
        connValues = connLoadProp [ propertyDict[eConn.props.TYPE] ]
        for val in connValues:
            propertyDict[ val ] = connValues[ val ]

    ## MERGE propertyDict with values from CONNECTION by tableName (Full DB loading !)
    if eConn.props.TBL in propertyDict and propertyDict[eConn.props.TBL] in connLoadProp:
        connValues = connLoadProp [ propertyDict[eConn.props.TBL] ]
        for val in connValues:
            propertyDict[ val ] = connValues[ val ]

    if propertyDict and isinstance(propertyDict, dict) and eConn.props.TYPE in propertyDict:
        cType = propertyDict[eConn.props.TYPE]
        if cType in CLASS_TO_LOAD:
            return CLASS_TO_LOAD[cType]( propertyDict=propertyDict )
        else:
            p("CONNECTION %s is NOT DEFINED. PROP: %s" % (str(cType), str(propertyDict)), "e")
    else:
        p ("connectorMng->mngConnectors: must have TYPE prop. prop: %s " %(str(propertyDict)), "e")

def __queryParsetIntoList(self, sqlScript, getPython=True, removeContent=True, dicProp=None, pythonWord="dingDONG"):
        if isinstance(sqlScript, (tuple, list)):
            sqlScript = "".join(sqlScript)
        # return list of sql (splitted by list of params)
        allQueries = self.__getAllQuery(longStr=sqlScript, splitParam=['GO', u';'])

        if getPython:
            allQueries = self.__getPythonParam(allQueries, mWorld=pythonWord)

        if removeContent:
            allQueries = self.__removeComments(allQueries)

        if dicProp:
            allQueries = self._replaceProp(allQueries, dicProp)

        return allQueries
