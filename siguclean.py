# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""

#VARIABLES DE CONFIGURACION
LDAP_SERVER = "ldaps://sunsrv.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
ORACLE_SERVER='ibmblade47/av10g'
DEFAUL_MOUNTS = {}

def getListByDate(toDate , fromDate='1900-01-01'):
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if DEBUG: print "getListByDate Query:",query
    cursor.execute(query)     
    userList = cursor.fetchall()
    return userList

def CheckEnvironment():
    print "Comprobando el entorno de ejecucion ..."
    CheckModules()
    con = CheckConnections()
    
def CheckModules():
    print "  Comprobando modulos necesarios"

    #python_ldap
    print('     comprobando modulo conexion a ldap  ... '),    
    try:
        global ldap
        import ldap
        print "CORRECTO"
    except:
        print "ERROR"
        print "No existe el modulo python-ldap, instalelo"
        exit

    #cx_Oracle
    print('     comprobando modulo conexion a Oracle ... '),    
    try:
        global cx_Oracle        
        import cx_Oracle
        print "CORRECTO"
    except:
        print "ERROR"
        print "No existe el modulo cx_Oracle, instalelo"
        exit
    
def CheckConnections():
    print "  Comprobando conexiones"

    #LDAP
    global WINDOWS_PASS
    WINDOWS_PASS = raw_input('     Introduzca la clave de windows (administrador):')
    print('     comprobando conexion a ldap ... '),
    try:
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, 0)
        ldapCon = ldap.initialize(LDAP_SERVER)
        ldapCon.simple_bind_s(BIND_DN, WINDOWS_PASS)
        print "CORRECTO"
    except ldap.LDAPError, e:
        print "ERROR"
        print e
        exit
    #Oracle
    global ORACLE_PASS
    ORACLE_PASS = raw_input('     Introduzca la clave de oracle (sigu):')
    print('     comprobando conexion a oracle ... '),
    try:
        oracleCon = cx_Oracle.connect('sigu/'+ORACLE_PASS+'@'+ORACLE_SERVER)
        print "CORRECTO"
    except:
        print "ERROR"
        exit
        
    return ldapCon, oracleCon

""" Aqui comprobamos el acceso a todos los FS y/o montajes implicados """    
def CheckMounts():
    print "  Comprobando el acceso a los Datos"
        
    
    
"""
Programa principal
"""
CheckEnvironment()
        