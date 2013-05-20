# -*- coding: utf-8 -*-
"""
Modulo de interfaz con la BBDD de sigu

Created on Wed May 15 12:13:01 2013

@author: tonin
"""
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
DEBUG = True

import cx_Oracle
import ldap
from pyad import pyad

con = cx_Oracle.connect("sigu/XXXXXXXXX@ibmblade47/av10g")
cursor = con.cursor()

def getListByDate(toDate , fromDate='1900-01-01'):
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if DEBUG: print "getListByDate Query:",query
    cursor.execute(query)     
    userList = cursor.fetchall()
    return userList

def testPyad():
    rows = getListByDate('2008-02-01')
    for usuario in rows:
        user = pyad.from_cn(usuario)
        print type(user)
        print user
        raw_input("Pulse para continuar ...")
    print len(rows)    
    

try:
   ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, 0)
   lcon = ldap.initialize(LDAP_SERVER)
   lcon.simple_bind_s(BIND_DN, BIND_PASS)
except ldap.LDAPError, e:
   print e

user = lcon.search_s(USER_BASE,ldap.SCOPE_SUBTREE,"cn=i02s*")

for dn,entry in user:
        print 'Processing',repr(dn)
        print entry
        #handle_ldap_entry(entry)
