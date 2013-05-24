# -*- coding: utf-8 -*-
"""
Created on Wed May 22 09:27:33 2013

@author: tonin
"""
class Status(object):
    def __init__(self,mandatory,optional):
        "Inicia todos los checks a false"
        self._fields_ = mandatory + optional 
        self._mandatory_ = mandatory  
        self._optional_ = optional            
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
        
    def add(self,field,value,tipo):
        if tipo == 'mandatory':
            self._mandatory_.append(field)
        else:
            self._optional_.append(field)
        self._fields_.append(field)
        if value:
            val='True'
        else:
            val='False'
        exec("self.%s = %s" % (field,val))

        
status=Status(["oracleCon",
                "ldapCon",
                "homenfs",
                "perfiles",
                "homecifs",
                "sessionId",
                "userList"],
                ["homemail"])
