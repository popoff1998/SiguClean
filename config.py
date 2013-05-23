# -*- coding: utf-8 -*-
"""
Created on Wed May 22 09:27:33 2013

@author: tonin
"""
class Status(object):
    _mandatory_ = ["oracleCon",
                "ldapCon",
                "homenfs",
                "perfiles",
                "homecifs",
                "sessionId",
                "userList"]
    _optional_ = ["homemail"]
                  
    _fields_ = _mandatory_ + _optional_                

    def __init__(self):
        "Inicia todos los checks a false"
        for field in self._fields_:
            exec("self.%s = False" % (field))
    def show(self):
        "Imprime todos los checks"
        for field in self._fields_:
            print field," = ",eval("self.%s" % (field))  
    def ok(self):
        "Evalua si todos los checks mandatory son correctos"
        for field in self._mandatory_:
            if eval("self.%s" % (field)) == False:
                return False
        return True

status=Status()