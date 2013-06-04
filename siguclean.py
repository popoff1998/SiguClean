# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
#from __future__ import print_function

#Defines
TEST = True
DEBUG = True
VERBOSE = 1

#VARIABLES DE CONFIGURACION
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
LDAP_SERVER = "ldaps://sunsrv.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
ORACLE_SERVER='ibmblade47/av10g'


HOMESNFS = 'INSTALA1CIONES'
HOMEMAIL = 'NEWMAIL/MAIL'

#PARAMETROS DE LA EJECUCION
if TEST:
    sessionId = "PRUEBA1"
    fromDate = ""
    toDate = "2012-01-01"

    MOUNTS = ({'account':'LINUX','fs':'homenfs','label':'HOMESNFSTEST','mandatory':True,'val':''},
              {'account':'MAIL','fs':'homemail','label':'NADADENADA','mandatory':False,'val':''})  
else:
    MOUNTS = ({'account':'LINUX','fs':'homenfs','label':'INSTALACIONES','mandatory':True,'val':''},
              {'account':'MAIL','fs':'homemail','label':'INSTALACIONES','mandatory':False,'val':''},  
              {'account':'WINDOWS','fs':'perfiles','label':'PERFILES$','mandatory':False,'val':''},  
              {'account':'WINDOWS','fs':'homecifs','label':'HOMESCIF','mandatory':True,'val':''})

    sessionId = ""
    fromDate = ""
    toDate = ""

import os,sys
from stat import *
from enum import Enum
import tarfile
from pprint import pprint
import config

state = Enum('NA','ARCHIVED','DELETED','TARFAIL','NOACCESIBLE','ROLLBACK','ERROR')

#FUNCIONES
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
        
def sizeToHuman(size):
    letter = ('B','K','M','G','T')
    indice = 0
    while(True):
        if DEBUG: print "DEBUG: (sizeToHuman) Size antes de redondear es: ",size 
        if size < 1024:
            string = str(size)+letter[indice]
            return string
        size = size / 1024
        indice = indice + 1
        
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

def getListByDate(toDate , fromDate='1900-01-01'):
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if DEBUG: print("DEBUG: getListByDate Query:",query)
    cursor = oracleCon.cursor()
    cursor.execute(query)     
    tmpList = cursor.fetchall()
    #Convertimos para quitar tuplas
    userList = [x[0] for x in tmpList]        
    config.status.userList = True
    return userList

#CLASES
    
class Session(object):
    def __init__(self,sessionId,fromDate,toDate):
        self.sessionId = sessionId
        self.fromDate = fromDate
        self.toDate = toDate
        self.accountList = []
        self.userList = []
        self.tarsizes = 0
        self.tardir = ''
        
    def getaccountList(self):
        if TEST:
            self.accountList = ['games','news','uucp']
        else:
            self.acountlist = getListByDate(self.toDate,self.fromDate)
    
    def start(self):
        #Directorio para TARS
        if DEBUG: print 'DEBUG: config.TARDIR es: ',config.TARDIR
        if os.path.exists(config.TARDIR):            
            if self.sessionId:            
                self.tardir = config.TARDIR + '/' + self.sessionId
            if os.path.isdir(self.tardir) is False:
                os.mkdir(self.tardir,0777)
        else:
            #Abortamos porque no existe el directorio padre de los tars
            Print(0,'ABORT: (session-start) No existe el directorio para tars: ',config.TARDIR)
        #Creo la lista de cuentas
        if not self.accountList:
            self.getaccountList()
        #Creo la lista de objetos usuario a partir de la lista de cuentas            
        if not self.userList:
            for account in self.accountList:
                user = User(account)
                self.userList.append(user) 
        #Proceso las entradas
        for user in self.userList:
            if user.check() is False:
                Print(0,"ABORT: Chequeando el usuario ", user.cuenta)
                exit(False)
            user.archive(self.tardir)
            self.tarsizes = self.tarsizes + user.tarsizes
            
        Print(1,'Tamaño de tars de la session ',self.sessionId,' es ',sizeToHuman(self.tarsizes))
        
class Storage(object):
    
    def __init__(self,key,path,mandatory,parent):
        self.key = key
        self.path = path
        self.mandatory = mandatory 
        self.tarpath = None
        self.parent = parent
        self.tarsize = 0
        self.state = state.NA
        self.accesible = self.exist()
        
    def display(self):
        Print(1,self.key,"\t = ",self.path,"\t Accesible: ",self.accesible,"\t Estado: ",self.state)

    def archive(self,rootpath):
        """ Archiva un storage en un tar"""
        #Vuelvo a comprobar aqui que es accesible
        if self.accesible is False:
            self.state = state.NOACCESIBLE
            return False
        self.tarpath = rootpath + '/' + self.parent.cuenta + '_' + self.key + '_' + sessionId + ".tar"
        Print(1,"Archiving ",self.key," from ",self.path," in ",self.tarpath," ... ")
        try:
            tar = tarfile.open(self.tarpath,"w:bz2")
            tar.add(self.path)
            tar.close()
            self.tarsize = os.path.getsize(self.tarpath)
            self.state = state.ARCHIVED
        except:
            Print(0,"ERROR: Archivando",self.key)
            self.state = state.TARFAIL
                
    def rollback(self):
        """Deshace el archivado borra los tars.
        - Si se ha borrado hacemos un untar
        - Borramos el tar
        - Ponemos el state como rollback"""
        if self.state == state.DELETED:
            if self.unarchive() is False:
                self.state = state.ERROR
                return False
        else:
            return False
        try:
            os.remove(self.tarpath)
            self.tarpath = None
            self.tarsize = 0
            self.state = state.ROLLBACK
            return True
        except:
            self.state = state.ERROR
            return False
            
    def unarchive(self):
        """Des-archiva un storage"""
        if self.state == state.DELETED or self.state == state.ARCHIVED:
            try:
                Print(1,"Unarchiving ",self.key," to ",self.path," from ",self.tarpath," ... ")                
                tar = tarfile.open(self.tarpath,"r:*")
                tar.extractall(self.path)
                tar.close()
                return True
            except:
                Print1(0,"Error unarchiving ",self.key," to ",self.path," from ",self.tarpath," ... ")
                return False

    def exist(self):
         """Comprueba la accesibilidad de un storage"""
         ret = True if os.path.exists(self.path) else False
         self.accesible = ret
         return ret

class User(object):
    global status    
    instancias = {}
    def __new__(cls,name):
        if name in User.instancias:
            return User.instancias[name]
        self = object.__new__(cls)
        User.instancias[name] = self
        return self    

    def check(self):
        """Metodo que chequea los storages mandatory del usuario
        Asumo que la DN está bien porque acabo de buscarla."""
        status = True
        for storage in self.storage:
            if storage.mandatory:
                if storage.exist() is False:
                    status = False
                    if DEBUG: print "DEBUG: NO DEBO ENTRAR AQUI SI MANDATORY ES FALSE O EL STORAGE EXISTE"
        return status
        
    def listCuentas(self):
        "Devuelve una tupla con las cuentas que tiene el usuario"
        return ('LINUX','MAIL') #dummy return
    
    def __init__(self,cuenta):
        try:
            dummy = self.cuenta
            if DEBUG: print "DEBUG: (user.__init__) YA EXISTIA USUARIO ",self.cuenta, " VUELVO DE INIT"
            return
        except:
            pass
        self.dn = None
        self.cuenta = cuenta
        self.storage = []
        for c in self.listCuentas():
            #relleno el diccionario storage
            for m in MOUNTS:
                #Si el montaje no esta paso a la siguiente cuenta
                if m['val'] is None:
                    continue
                if c == m['account']:
                    sto_path = m['val'] + '/' + self.cuenta
                    sto_key = m['fs']
                    #Trato el caso especial de mail
                    if c == 'MAIL':
                        if os.path.islink(sto_path):
                            storage = Storage('MAILLINK',sto_path,False,self)
                            self.storage.append(storage)
                            sto_path = os.path.realpath(sto_path)
                    #Caso especial de WINDOWS (calcular dn)
                    if c == 'WINDOWS':
                        status,dn,result_type = dnFromUser(self.cuenta)
                        if status:
                            self.dn = dn
                        else:
                            self.dn = False                       
                    storage = Storage(sto_key,sto_path,m['mandatory'],self)
                    self.storage.append(storage)

    def showstorage(self):
        for storage in self.storage:
            storage.display()

    def show(self):
        Print(1,'Cuenta\t=\t',self.cuenta)
        Print(1,'DN\t=\t',self.dn)
        self.showstorage()
            
    def rollback(self):
        """Metodo para hacer rollback de lo archivado
        El rollback consiste en:
            - Recuperar de los tars los storages borrados            
            - Borrar los tars
        """
        for storage in self.storage:
            if storage.rollback() is False:
                """Hay un error irrecuperable en el rollback
                Es mejor abortar"""
                Print(0,"ERROR Irrecuperable en rollback, abortamos")
                exit(1)
                
    def archive(self,tardir):
        self.tarsizes = 0
        #pendiente de controlar errores y mandatory
        "Metodo que archiva todos los storages del usuario"
        if os.path.isdir(tardir) is False:
            Print(0,"ERROR: (user-archive) No existe el directorio para TARS",tardir)
            exit
    
        rootpath = tardir + '/' + self.cuenta
        if os.path.isdir(rootpath) is False:
            os.mkdir(rootpath,0777)
    
        for storage in self.storage:
            storage.archive(rootpath)
            self.tarsizes = self.tarsizes + storage.tarsize
        Print(2,'El tamaño de los tars para ',self.cuenta,' es: ',self.tarsizes)
                


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
        usuario = User(line)
        usuario.show()

    def do_checkuser(self,line):
        usuario = User(line)
        status = usuario.check()
        usuario.showstorage()
        Print(1,"El estado del usuario para borrar es: ",status)
        
    def do_archive(self,line):
        usuario = User(line)
        usuario.archive(config.TARDIR)

    def do_startsession(self,line):
        if DEBUG: print "DEBUG: sessionID ",sessionId
        ses = Session(sessionId,fromDate,toDate)
        ses.start()
        
    def __init__(self):
        cmd.Cmd.__init__(self)
        

"""
Programa principal
"""
shell().cmdloop()




    
    

     