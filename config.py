# -*- coding: utf-8 -*-
"""
Created on Wed May 22 09:27:33 2013

@author: tonin
"""
class Status(object):
    _fields_ = ["oracleCon",
                "ldapCon",
                "homenfs",
                "homemail",
                "perfiles",
                "homecifs",
                "toDate",
                "fromDate",
                "sessionId",
                "userList"]
                
    def __init__(self):
        for field in self._fields_:
            exec("self.%s = False" % (field))
    def show(self):
        for field in self._fields_:
            print field," = ",eval("self.%s" % (field))      

status=Status()