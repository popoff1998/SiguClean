# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
DEBUG = True
#VARIABLES DE CONFIGURACION
LDAP_SERVER = "ldaps://sunsrv.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
ORACLE_SERVER='ibmblade47/av10g'

HOMESNFS = 'INSTALA1CIONES'
HOMEMAIL = 'NEWMAIL/MAIL'

#PARAMETROS DE LA EJECUCION
sessionId = ""
fromDate = ""
toDate = ""

#MOUNTS = ({'fs':'homenfs','label':'HOMESNFS','val':''},
#          {'fs':'homemail','label':'NEWMAIL/MAIL','val':''})  

MOUNTS = ({'fs':'homenfs','label':'INSTALACIONES','val':''},
          {'fs':'homemail','label':'NEWMAIL/MAIL','val':''})  

import os


def getListByDate(toDate , fromDate='1900-01-01'):
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if DEBUG: print "getListByDate Query:",query
    cursor = oracleCon.cursor()
    cursor.execute(query)     
    userList = cursor.fetchall()
    return userList

def CheckEnvironment():
    print "PASO1: Comprobando el entorno de ejecucion ..."
    CheckModules()
    con = CheckConnections()
    CheckMounts()
    
    
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
        global ldapCon
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
        global oracleCon
        oracleCon = cx_Oracle.connect('sigu/'+ORACLE_PASS+'@'+ORACLE_SERVER)
        print "CORRECTO"
    except:
        print "ERROR"
        exit
        
    return ldapCon, oracleCon

def get_mount_point(algo):
    "Devuelve el punto de montaje que contiene algo en el export"
    try:
        with open("/proc/mounts", "r") as ifp:
            for line in ifp:
                fields= line.rstrip('\n').split()
                if algo in fields[0]: 
                    return fields[1]
    except EnvironmentError:
        pass
    return None # explicit
    
def CheckMounts():
    "Comprueba que los puntos de montaje están accesibles"

    print "  Comprobando el acceso a los Datos"
    salgo = False
    for var in MOUNTS:
        print('     comprobando '+var['fs']+' ...'),
        var['val'] = get_mount_point(var['label'])
        if var['val'] != None:
            print var['val']
        else:
            print "NO ACCESIBLE"
            salgo = True
    if salgo:
        exit

def inputParameter(param,text,mandatory):
    "Lee un parametro admitiendo que la tecla intro ponga el anterior"
    while True:
        prevParam = param
        param = raw_input(text+'['+param+']: ')
        if param == '':
            param = prevParam
        if param == 'c':
            param = ''
        if param == '' and mandatory:
            continue
        else:
            return param
            
def EnterParameters():
    "Lee por teclado los parametros de ejecucion"
    while True:
        global sessionId,fromDate,toDate
        print "PASO2: Parametros de la sesion ('c' para borrar)"
        sessionId = inputParameter(sessionId,"Identificador de sesion: ",True)      
        fromDate = inputParameter(fromDate,"Fecha desde (yyyy-mm-dd): ",False)
        toDate = inputParameter(toDate,"Fecha hasta (yyyy-mm-dd): ",True)
        
        print '\nSessionId = ['+sessionId+']'
        print 'fromDate = ['+fromDate+']'
        print 'toDate = ['+toDate+']'
        
        sal = raw_input('\nSon Correctos (S/n): ')
        if sal == 'S':
            return
        else:
            continue
        
def pager(iterable, page_size):
    import itertools 
    args = [iter(iterable)] * page_size
    fillvalue = object()
    for group in itertools.izip_longest(fillvalue=fillvalue, *args):
        yield (elem for elem in group if elem is not fillvalue)  
        
        
        
           
    
    
"""
Programa principal
"""
CheckEnvironment()
EnterParameters()
if fromDate != '':
    userList = getListByDate(toDate,fromDate)
else:
    userList = getListByDate(toDate)
    
print "\nEl numero de usuarios a borrar es ",len(userList)

my_pager = pager(userList,20)
for page in my_pager:
    for i in page:
        print i
    tecla = raw_input("----- Pulse intro para continuar (q para salir) ----------")
    if tecla == 'q':
        break

    
    

     