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
global userList

#MOUNTS = ({'fs':'homenfs','label':'HOMESNFS','val':''},
#          {'fs':'homemail','label':'NEWMAIL/MAIL','val':''})  

MOUNTS = ({'account':'LINUX','fs':'homenfs','label':'INSTALACIONES','val':''},
          {'account':'MAIL','fs':'homemail','label':'NEWMAIL/MAIL','val':''},  
          {'account':'WINDOWS','fs':'perfiles','label':'PERFILES$','val':''},  
          {'account':'WINDOWS','fs':'homecifs','label':'HOMESCIF$','val':''})

import os,sys
from stat import *
import config

def dnFromUser(user):
    filtro = "(&(CN="+user+")(!(objectClass=contact)))"  
    print "User: ",user
    print "Filtro: ",filtro
    result_id = ldapCon.search(USER_BASE,
                            ldap.SCOPE_SUBTREE,
                            filtro,
                            ["dn"],
                            1)
    print "DESPUES DE RESULT"
                            
    none,tupla = ldapCon.result(result_id,0)
    dn,none = tupla[0]
    return dn
                
class user(object):
    cuenta = None    
    dn = None
    storage = {}
    
    def __init__(self,cuenta):
        self.cuenta = cuenta

        for c in userCuentas(cuenta):
            #relleno el diccionario storage
            for m in MOUNTS:
                if c == m['account']:
                    sto_path = m['val'] + '/' + cuenta
                    sto_key = m['fs']
                    #Trato el caso especial de mail
                    if c == 'MAIL':
                        if os.path.islink(sto_path):
                            self.storage['MAILLINK'] = sto_path
                            sto_path = os.path.realpath(sto_path)
                    self.storage[sto_key] = sto_path
            #Hallo el dn en el caso de cuentas windows
                                 
                
    
def getListByDate(toDate , fromDate='1900-01-01'):
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if DEBUG: print "getListByDate Query:",query
    cursor = oracleCon.cursor()
    cursor.execute(query)     
    userList = cursor.fetchall()
    config.status.userList = True
    return userList

def CheckEnvironment():
    print "PASO1: Comprobando el entorno de ejecucion ..."
    CheckModules()
    CheckConnections()
    CheckMounts()
    
    
def CheckModules():
    "Comprueba que son importables los módulos ldap y cx_Oracle"
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
    "Establece las conexiones a ldap y oracle"
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
        config.status.ldapCon = True
        print "CORRECTO"
    except ldap.LDAPError, e:
        print "ERROR"
        print e
        config.status.ldapCon = False
    #Oracle
    global ORACLE_PASS
    ORACLE_PASS = raw_input('     Introduzca la clave de oracle (sigu):')
    print('     comprobando conexion a oracle ... '),
    try:
        global oracleCon
        oracleCon = cx_Oracle.connect('sigu/'+ORACLE_PASS+'@'+ORACLE_SERVER)
        config.status.oracleCon = True
        print "CORRECTO"
    except:
        print "ERROR"
        config.status.oracleCon = False

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
            exec("config.status.%s = True" % (var['fs']))
        else:
            exec("config.status.%s = False" % (var['fs']))
            print "NO ACCESIBLE"
            salgo = True
    if salgo:
        exit
        
def userCuentas(user):
    "Devuelve una tupla con las cuentas que tiene el usuario"
    
def checkUser(user):
    """ Comprueba que el usuario cumple todas las condiciones para ser borrado
        Las condiciones son:
            a) Accesibilidad a todos los storages asociados con sus cuentas
            b) Accesibilidad al objeto LDAP de AD si tiene cuenta AD
     """       
    for cuenta in userCuentas(user):
        checkStorage(cuenta)

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
            config.status.sessionId = True
            return
        else:
            continue
        
def pager(iterable, page_size):
    import itertools 
    args = [iter(iterable)] * page_size
    fillvalue = object()
    for group in itertools.izip_longest(fillvalue=fillvalue, *args):
        yield (elem for elem in group if elem is not fillvalue)  

def imprimeUsuarios(userList):        
    my_pager = pager(userList,20)
    for page in my_pager:
        for i in page:
            print i
        tecla = raw_input("----- Pulse intro para continuar (q para salir) ----------")
        if tecla == 'q':
            break       

import cmd        
class shell(cmd.Cmd):
    def do_check(self, line):
        CheckEnvironment()
        
    def do_getusers(self, line):
        if fromDate != '':
            userList = getListByDate(toDate,fromDate)
        else:
            userList = getListByDate(toDate)
        print "\nEl numero de usuarios a borrar es ",len(userList)
        
    def do_params(self, line):
        EnterParameters()
    
    def do_printusers(self, line):
        imprimeUsuarios(userList) 
        
    def do_quit(self, line):
        return True

    def do_status(self,line):
        config.status.show()
        print "El estado OK es ",config.status.ok()
    
    def do_dnfromuser(self,line):
        print dnFromUser(line)
        
    def __init__(self):
        cmd.Cmd.__init__(self)
"""
Programa principal
"""
shell().cmdloop()




    
    

     