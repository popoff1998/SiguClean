# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
#from __future__ import print_function

#Defines
DEBUG = True
VERBOSE = 1
TARDIR='/tmp'

#VARIABLES DE CONFIGURACION
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
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

MOUNTS = ({'account':'LINUX','fs':'homenfs','label':'INSTALACIONES','mandatory':True,'val':''},
          {'account':'MAIL','fs':'homemail','label':'INSTALACIONES','mandatory':False,'val':''},  
          {'account':'WINDOWS','fs':'perfiles','label':'PERFILES$','mandatory':False,'val':''},  
          {'account':'WINDOWS','fs':'homecifs','label':'HOMESCIF','mandatory':True,'val':''})

import os,sys
from stat import *
from enum import Enum
import tarfile
from pprint import pprint
import config

state = Enum('NA','COPY','FAIL','SKIP','ROLLBACK')

def Print(level,*args,**kwargs):
    if VERBOSE >= level:
        if kwargs != {}:
            trail = kwargs['end']
        else:
            trail = '\n'
        for string in args:
            print(string),
        print(trail),
        
def dnFromUser(user):
    filtro = "(&(CN="+user+")(!(objectClass=contact)))"  
    try:
        result_id = ldapCon.search(USER_BASE,
                            ldap.SCOPE_SUBTREE,
                            filtro,
                            ["dn"],
                            1)
        result_type,tupla = ldapCon.result(result_id,0)
        dn,none = tupla[0]
        status = True
    except:
        status = False
    return status,dn,result_type
    
class Storage(object):
    global state
    def __init__(self,key,path,mandatory,parent):
        self.key = key
        self.path = path
        self.accesible = None
        self.mandatory = mandatory        
        self.state = state.NA
        self.parent = parent
        self.tarsize = 0
        
    def display(self):
        Print(1,self.key,"\t = ",self.path,"\t Accesible: ",self.accesible,"\t Estado: ",self.state)

    def archive(self,rootpath):
            #Vuelvo a comprobar aqui que es accesible
            if os.path.exists(self.path) is False:
                #Aquí vendra el manejo del error                
                return False
            tarpath = rootpath + '/' + self.parent.cuenta + '_' + self.key + '_' + sessionId + ".tar"
            Print(1,"Archiving ",self.key," from ",self.path," in ",tarpath," ... ")
            tar = tarfile.open(tarpath,"w")
            tar.add(self.path)
            self.tarsize = os.path.getsize(tarpath)
                
class user(object):
    def check(self):
        """Metodo que chequea los storages mandatory del usuario
        Asumo que la DN está bien porque acabo de buscarla."""
        status = True
        for storage in self.storage:
            if os.path.exists(storage.path) is False:
                storage.accesible = False
                status = False
            else:
                storage.accesible = True
        return status
        
    def __init__(self,cuenta):
        self.dn = None
        self.cuenta = cuenta
        self.storage = []
        for c in userCuentas(cuenta):
            #relleno el diccionario storage
            for m in MOUNTS:
                if c == m['account']:
                    sto_path = m['val'] + '/' + cuenta
                    sto_key = m['fs']
                    #Trato el caso especial de mail
                    if c == 'MAIL':
                        if os.path.islink(sto_path):
                            storage = Storage('MAILLINK',sto_path,False,self)
                            self.storage.append(storage)
                            sto_path = os.path.realpath(sto_path)
                    #Caso especial de WINDOWS (calcular dn)
                    if c == 'WINDOWS':
                        status,dn,result_type = dnFromUser(cuenta)
                        if status:
                            self.dn = dn
                        else:
                            self.dn = False                       
                    storage = Storage(sto_key,sto_path,m['mandatory'],self)
                    self.storage.append(storage)

    def showstorage(self):
        for storage in self.storage:
            storage.display()
            
    def rollback(self):
        "Metodo para hacer rollback de lo archivado"
        
    def archive(self):
        self.tarsizes = 0
        #pendiente de controlar errores y mandatory
        "Metodo que archiva todos los storages del usuario"
        if os.path.isdir(TARDIR) is False:
            Print(0,"ERROR: No existe el directorio para TARS")
            exit
    
        rootpath = TARDIR + '/' + self.cuenta
        if os.path.isdir(rootpath) is False:
            os.mkdir(rootpath,0777)
    
        for storage in self.storage:
            storage.archive(rootpath)
            self.tarsizes = self.tarsizes + storage.tarsize
        Print(1,"El tamaño de los tars es: ",self.tarsizes)
                
def getListByDate(toDate , fromDate='1900-01-01'):
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if DEBUG: print("getListByDate Query:",query)
    cursor = oracleCon.cursor()
    cursor.execute(query)     
    tmpList = cursor.fetchall()
    #Convertimos para quitar tuplas
    userList = [x[0] for x in tmpList]        
    config.status.userList = True
    return userList

def CheckEnvironment():
    Print(1,"PASO1: Comprobando el entorno de ejecucion ...")
    CheckModules()
    CheckConnections()
    CheckMounts()
    
    
def CheckModules():
    "Comprueba que son importables los módulos ldap y cx_Oracle"
    Print(1,"  Comprobando modulos necesarios")

    #python_ldap
    Print(1,'     comprobando modulo conexion a ldap  ... ',end=' ')    
    try:
        global ldap
        import ldap
        Print(1,"CORRECTO")
    except:
        print(0,"ERROR: No existe el modulo python-ldap, instalelo")
        exit

    #cx_Oracle
    Print(1,'     comprobando modulo conexion a Oracle ... ',end='')    
    try:
        global cx_Oracle        
        import cx_Oracle
        Print(1,"CORRECTO")
    except:
        print('ERROR: No existe el modulo cx_Oracle, instalelo')
        exit
    
def CheckConnections():
    "Establece las conexiones a ldap y oracle"
    Print(1,"  Comprobando conexiones")

    #LDAP
    global WINDOWS_PASS
    WINDOWS_PASS = raw_input('     Introduzca la clave de windows (administrador):')
    Print(1,'     comprobando conexion a ldap ... ',end='')
    try:
        global ldapCon
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, 0)
        ldap.set_option(ldap.OPT_REFERRALS,0)
        ldapCon = ldap.initialize(LDAP_SERVER)
        ldapCon.simple_bind_s(BIND_DN, WINDOWS_PASS)
        config.status.ldapCon = True
        Print(1,"CORRECTO")
    except ldap.LDAPError, e:
        Print(1,"ERROR")
        Print(e)
        config.status.ldapCon = False
    #Oracle
    global ORACLE_PASS
    ORACLE_PASS = raw_input('     Introduzca la clave de oracle (sigu):')
    Print(1,'     comprobando conexion a oracle ... ',end='')
    try:
        global oracleCon
        oracleCon = cx_Oracle.connect('sigu/'+ORACLE_PASS+'@'+ORACLE_SERVER)
        config.status.oracleCon = True
        Print(1,"CORRECTO")
    except:
        Print(1,"ERROR")
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
    Print(1,"  Comprobando el acceso a los Datos")
    salgo = False
    for var in MOUNTS:
        Print(1,'     comprobando '+var['fs']+' ...',end='')
        var['val'] = get_mount_point(var['label'])
        if var['val'] != None:
            Print(1,var['val'])
            exec("config.status.%s = True" % (var['fs']))
        else:
            exec("config.status.%s = False" % (var['fs']))
            Print(1,"NO ACCESIBLE")
            salgo = True
    if salgo:
        exit
        
def userCuentas(user):
    "Devuelve una tupla con las cuentas que tiene el usuario"
    return ('WINDOWS','LINUX','MAIL') #dummy return
    
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
        Print(1,"PASO2: Parametros de la sesion ('c' para borrar)")
        sessionId = inputParameter(sessionId,"Identificador de sesion: ",True)      
        fromDate = inputParameter(fromDate,"Fecha desde (yyyy-mm-dd): ",False)
        toDate = inputParameter(toDate,"Fecha hasta (yyyy-mm-dd): ",True)
        
        Print(1,'\nSessionId = ['+sessionId+']')
        Print(1,'fromDate = ['+fromDate+']')
        Print(1,'toDate = ['+toDate+']')
        
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

def imprime(userList):        
    my_pager = pager(userList,20)
    for page in my_pager:
        for i in page:
            Print(1,i)
        tecla = raw_input("----- Pulse intro para continuar (q para salir) ----------")
        if tecla == 'q':
            break       

import cmd        
class shell(cmd.Cmd):
    def do_check(self, line):
        CheckEnvironment()
        
    def do_getusers(self, line):
        global userList
        if fromDate != '':
            userList = getListByDate(toDate,fromDate)
        else:
            userList = getListByDate(toDate)
        Print(1,"\nEl numero de usuarios a borrar es ",len(userList))
        
    def do_params(self, line):
        EnterParameters()
    
    def do_printusers(self, line):
        imprime(userList) 
        
    def do_quit(self, line):
        return True

    def do_status(self,line):
        config.status.show()
        Print(1,"El estado OK es ",config.status.ok())
    
    def do_dnfromuser(self,line):
        status,dn,result_type = dnFromUser(line)
        if status:
            Print(1,dn)
        else:
            Print(1,"ERROR; resultype:",result_type,"Dn: ",dn)
        
    def do_printdns(self,line):
        for user in userList:
            status,dn,result_type = dnFromUser(user)
            if status:
                Print(1,dn)
            else:
                Print(1,"ERROR; resultype:",result_type,"Dn: ",dn)
    
    def do_showuser(self,line):
        usuario = user(line)
        pprint(vars(usuario))

    def do_checkuser(self,line):
        usuario = user(line)
        status = usuario.check()
        usuario.showstorage()
        Print(1,"El estado del usuario para borrar es: ",status)
        
    def do_archive(self,line):
        usuario = user(line)
        usuario.archive()
        
    def __init__(self):
        cmd.Cmd.__init__(self)
"""
Programa principal
"""
shell().cmdloop()




    
    

     