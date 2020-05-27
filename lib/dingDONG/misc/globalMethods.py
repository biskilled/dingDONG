import sys
import re

from collections import OrderedDict
from dingDONG.config import config


def setProperty (k, o, defVal=None, setVal=None):
    def propInObject(o):
        if hasattr(o, '__dict__'):
            for prop in o.__dict__:
                if len(prop) > 2 and '__' == prop[0:2]:
                    continue
                else:
                    if prop.lower().replace('_', '') == k.lower().replace('_', ''):
                        return o.__dict__[prop]

    if setVal:
        return setVal

    if isinstance(o, (dict, OrderedDict)):
        lDict = {str(x).lower().replace('_', ''): x for x in o}

        if k.lower().replace('_', '') in lDict:
            if o[lDict[k.lower().replace('_', '')]]: return o[lDict[k.lower().replace('_', '')]]

    ret = propInObject(o)
    if ret: return ret

    ret = propInObject(config)
    if ret: return ret

    return defVal

def findEnum (prop, obj):
    dicClass = obj.__dict__
    def getPropValue (prop):
        if prop:
            for p in dicClass:
                if isinstance(dicClass[p], str) and dicClass[p].lower() == str(prop).lower():
                    return prop
                elif isinstance(dicClass[p], int) and str(dicClass[p]) == str(prop):
                    return prop

                if isinstance(dicClass[p], dict):
                    for k in dicClass[p]:
                        if dicClass[p][k] and prop in dicClass[p][k]:
                            return k
        return None

    return  getPropValue (prop)

def getAllProp (obj):
    dicClass = obj.__dict__
    ret = []
    for p in dicClass:
        if '__' not in p and isinstance(dicClass[p], str):
            ret.append ( dicClass[p] )
    return ret

def uniocdeStr (sObj, decode=False):
    if 3 == sys.version_info[0]:
        return sObj

    return unicode (str(sObj).decode(config.DECODE)) if decode else unicode (sObj)

def replaceStr (sString,findStr, repStr, ignoreCase=True,addQuotes=None):
    if addQuotes and isinstance(repStr,str):
        repStr="%s%s%s" %(addQuotes,repStr,addQuotes)

    if ignoreCase:
        pattern = re.compile(re.escape(findStr), re.IGNORECASE)
        res = pattern.sub (repStr, sString)
    else:
        res = sString.replace (findStr, repStr)
    return res