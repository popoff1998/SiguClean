#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
#from __future__ import print_function

#Defines
TEST = False
DEBUG = False
EXTRADEBUG = False
VERBOSE = 1
DRYRUN = False
SOFTRUN = False
CONFIRM = False
PROGRESS = False

#VARIABLES DE CONFIGURACION
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
Q_GET_CUENTA_NT = 'SELECT CCUENTA FROM UT_CUENTAS_NT WHERE CCUENTA ='
Q_INSERT_STORAGE = 'INSERT INTO UT_ST_STORAGE (IDSESION,CCUENTA,TTAR,NSIZE,CESTADO) VALUES '
Q_INSERT_SESION = 'INSERT INTO UT_ST_SESION (IDSESION,FSESION,FINICIAL,FFINAL,DSESION) VALUES '

#LDAP_SERVER = "ldap://ldap1.priv.uco.es"
LDAP_SERVER = "ldaps://ucoapp08.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
ORACLE_SERVER='ibmblade47/av10g'
ALTROOTPREFIX = '0_'
MAXSIZE = 0
NTCHECK = 'ad'
#Claves
WINDOWS_PASS = None
ORACLE_PASS = None

#Control del abort
"""
ABORTLIMIT: Numero de fallos admitidos
ABORTDECREASE: Si un borrado Ok debe decrementar la cuenta de fallos
ABORTALWAYS: Si cualquier fallo debe abortar
ABORTINSEVERITY: Si un fallo con severidad debe abortar
"""
ABORTLIMIT      = 5
ABORTDECREASE   = True
ABORTALWAYS     = False
ABORTINSEVERITY = False
fDebug = None

#Filtro de exclusion de momtajes, lo inicializamos como algo imposible de cumplir
MOUNT_EXCLUDE = "(?=a)b"

#PARAMETROS DE LA EJECUCION
if TEST:
    sessionId = "PRUEBA1"
    fromDate = ""
    toDate = "2012-01-01"

    MOUNTS = ({'account':'LINUX','fs':'homenfs','label':'HOMESNFSTEST','mandatory':True,'val':''},
              {'account':'MAIL','fs':'homemail','label':'NEWMAILTEST','mandatory':False,'val':''})  
else:
    MOUNTS = ({'account':'LINUX','fs':'homenfs','label':'HOMESNFS','mandatory':True,'val':''},
              {'account':'MAIL','fs':'homemail','label':'MAIL','mandatory':True,'val':''},  
              {'account':'WINDOWS','fs':'perfiles','label':'PERFILES','mandatory':False,'val':''},  
              {'account':'WINDOWS','fs':'homecifs','label':'HOMESCIF','mandatory':True,'val':''})

    sessionId = ""
    fromDate = ""
    toDate = ""

import os,sys
from shutil import rmtree
from stat import *
from enum import Enum
import tarfile
from pprint import pprint
import config
import pickle
import datetime
import dateutil.parser
import re
import collections
from progressbar import *

state = Enum('NA','ARCHIVED','DELETED','TARFAIL','NOACCESIBLE','ROLLBACK','ERROR','DELETEERROR')

#FUNCIONES
def haveprogress():
    if PROGRESS and not DEBUG and VERBOSE == 0: 
        return True
    else:
        return False
        
def confirm():
    #Pide confirmacion por teclado
    a = raw_input("Desea continuar S/N (N)")
    if a == "S":
        return True
    else:
        exit(True)
        
def iterable(obj):
    if isinstance(obj, collections.Iterable):
        return True
    return False
    
def Pprint(*args):
    for arg in args:
        if iterable(arg): 
            pprint(arg),
        else:
            print(arg),

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
        print(0,"ABORT: No existe el modulo python-ldap, instalelo")
        exit(False)

    #cx_Oracle
    Print(1,'     comprobando modulo conexion a Oracle ... ',end='')    
    try:
        global cx_Oracle        
        import cx_Oracle
        Print(1,"CORRECTO")
    except:
        print('ABORT: No existe el modulo cx_Oracle, instalelo')
        exit(False)
    
def CheckConnections():
    "Establece las conexiones a ldap y oracle"
    Print(1,"  Comprobando conexiones")
    import ldap,cx_Oracle
    #LDAP
    global WINDOWS_PASS
    if not WINDOWS_PASS:
        WINDOWS_PASS = raw_input('     Introduzca la clave de windows (administrador): ')
    if WINDOWS_PASS != "dummy":
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
    if not ORACLE_PASS:
        ORACLE_PASS = raw_input('     Introduzca la clave de oracle (sigu): ')
    if ORACLE_PASS != "dummy":
        Print(1,'     comprobando conexion a oracle ... ',end='')
        try:
            global oracleCon
            oracleCon = cx_Oracle.connect('sigu/'+ORACLE_PASS+'@'+ORACLE_SERVER)
            config.status.oracleCon = True
            Print(1,"CORRECTO")
        except:
            Print(1,"ERROR")
            config.status.oracleCon = False

def get_mount_point(algo,exclude_regex):
    "Devuelve el punto de montaje que contiene algo en el export"
    try:
        with open("/proc/mounts", "r") as ifp:
            for line in ifp:
                fields= line.rstrip('\n').split()
                if DEBUG: Debug("DEBUG-INFO: EXPORT: ",fields[0]," MOUNT: ",fields[1]," ALGO: ",algo)
                if algo in fields[0]: 
                    #Es un posible montaje, vemos si esta excluido
                    ret = exclude_regex.search(fields[1])
                    if DEBUG: Debug("DEBUG-INFO: (getmountpoint): campo es ",fields[1]," ret es ",ret)
                    if ret is not None:
                        if DEBUG: Debug("DEBUG-INFO: EXCLUIDO")
                        pass
                    else:
                        if DEBUG: Debug("DEBUG-INFO: INCLUIDO")
                        return fields[1]
    except EnvironmentError:
        pass
    return None # explicit
    
def CheckMounts():
    "Comprueba que los puntos de montaje están accesibles"
    Print(1,"  Comprobando el acceso a los Datos")
    try:
        regex = re.compile(MOUNT_EXCLUDE)
        if DEBUG: Debug("DEBUG-INFO: Regex de exclusion es ",MOUNT_EXCLUDE," y su valor es ",regex)
    except:
        Print(0,"La expresion ",MOUNT_EXCLUDE," no es una regex valida, abortamos ...")
        exit(False)
    salgo = False
    for var in MOUNTS:
        Print(1,'     comprobando '+var['fs']+' ...',end='')
        var['val'] = get_mount_point(var['label'],regex)
        if var['val'] != None:
            Print(1,"Usando montaje ",var['val'])
            exec("config.status.%s = True" % (var['fs']))
        else:
            exec("config.status.%s = False" % (var['fs']))
            Print(1,"NO ACCESIBLE")
            salgo = True
    if salgo:
        Print(0,'ABORT: Algunos puntos de montaje no estan accesibles')
        exit(False)
    if CONFIRM: confirm()

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

def imprime(userList):        
    my_pager = pager(userList,20)
    for page in my_pager:
        for i in page:
            Print(1,i)
        tecla = raw_input("----- Pulse intro para continuar (q para salir) ----------")
        if tecla == 'q':
            break       
        
def sizeToHuman(size):
    symbols = ('B','K','M','G','T')
    indice = 0
    if EXTRADEBUG: Debug("DEBUG-INFO: (sizeToHuman) Size antes de redondear es: ",size )
    while(True):
        if size < 1024:
            string = str(size)+symbols[indice]
            return string
        size = size / 1024
        indice = indice + 1
        
def humanToSize(size):
    symbols = ('B','K','M','G','T')
    letter = size[-1:].strip().upper()
    num = size[:-1]
    try:
        assert num.isdigit() and letter in symbols
        num = float(num)
        prefix = {symbols[0]:1}
        for i, s in enumerate(symbols[1:]):
            prefix[s] = 1 << (i+1)*10
        return int(num * prefix[letter])  
    except:
        if DEBUG: Debug('DEBUG-ERROR: (humanToSize) ',size,' no es traducible')
        return False
        
def timeStamp():
    return '['+str(datetime.datetime.now())+']\t'
    
def Print(level,*args,**kwargs):
    global VERBOSE
    if not VERBOSE: VERBOSE = 0
    if kwargs != {}:
        trail = kwargs['end']
    else:
        trail = '\n'
    cadena = "".join(str(x) for x in args)
    if VERBOSE >= level: print cadena+trail,
    
    if config.session:
        if hasattr(config.session,'log'):
            config.session.log.writeLog(cadena+trail,False)
        
def Debug(*args,**kwargs):
    global fDebug
    #Si tenemos verbose o no tenemos sesion sacamos la info por consola tambien
    if VERBOSE>0 or not config.session:
        print "".join(str(x) for x in args)
    #Si tenemos definida la sesion lo grabamos en el fichero
    if config.session:
        if not fDebug:
            fDebug = open(config.session.tardir+"/debug","w")
        if kwargs != {}:
            trail = kwargs['end']
        else:
            trail = '\n'
        fDebug.write(timeStamp())
        for string in args:
            fDebug.write(str(string))
        fDebug.write(trail)
        fDebug.flush()
    
def dnFromUser(user):
    import ldap 
    
    filtro = "(&(CN="+user+")(!(objectClass=contact)))"  
    try:
        result_id = ldapCon.search(USER_BASE,
                            ldap.SCOPE_SUBTREE,
                            filtro,
                            None)
        result_type,tupla = ldapCon.result(result_id,1)
        dn,none = tupla[0]
        status = True
    except:
        status = False
    return status,dn,tupla,result_type

def ldapFromSigu(cuenta,attr):
    Q_LDAP_SIGU = 'select sigu.ldap.uf_leeldap(\''+cuenta+'\',\''+attr+'\') from dual'
    
    cursor = oracleCon.cursor()
    cursor.execute(Q_LDAP_SIGU)     
    tmpList = cursor.fetchall()
    tmpList = tmpList[0][0]
    if DEBUG: Debug("DEBUG-INFO (ldapFromSigu): tmplist = ",tmpList)
    return tmpList.strip().split(':')[1].strip() if tmpList else None
    
def getListByDate(toDate , fromDate='1900-01-01'):
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if DEBUG: Debug("DEBUG-INFO: (getListByDate) Query:",query)
    try:
        cursor = oracleCon.cursor()
        cursor.execute(query)     
        tmpList = cursor.fetchall()
        cursor.close()
    except BaseException,e:
        Print(0,"ERROR: Error recuperando la lista de usuarios")
        if DEBUG: Debug("DEBUG-ERROR: (getListByDate): ",e)
        return None
    #Convertimos para quitar tuplas
    userList = [x[0] for x in tmpList]        
    config.status.userList = True
    return userList
    
def isArchived(user):
    try:
        cursor = oracleCon.cursor()
        #Llamo a la función uf_st_ultima
        ret = cursor.callfunc('UF_ST_ULTIMA',cx_Oracle.STRING,user)
        print "RET: ",ret
        cursor.close()    
        return ret
    except BaseException,e:
        if DEBUG: Debug("DEBUG-ERROR: (isArchived) ",e)
        return False
        
def hasCuentaNT(cuenta):
    import ldap 
    statAD = False
    statSigu = False
    
    if NTCHECK == 'sigu' or NTCHECK == 'both':
        query = Q_GET_CUENTA_NT + '\'' + cuenta + '\''
        cursor = oracleCon.cursor()
        cursor.execute(query)     
        statSigu = cursor.fetchall()
        
    if NTCHECK == 'ad' or NTCHECK == 'both':
        filtro = "(&(CN="+cuenta+")(!(objectClass=contact)))"  
        try:
            result_id = ldapCon.search(USER_BASE,
                                ldap.SCOPE_SUBTREE,
                                filtro,
                                None)
            result_type,tupla = ldapCon.result(result_id,1)
            if len(tupla) == 1:
                statAD = False
            else:
                statAD = True
        except:
            statAD = False
            
    if NTCHECK == 'sigu': return True if statSigu else  False
    if NTCHECK == 'ad': return statAD
    return True if statAD and statSigu else  False
    
def comillas(cadena):
    return '\''+cadena+'\''

def valueslist(*args,**kwargs):
    cadena = '('
    size = len(args)
    argnumber = 1
    for x in args:
        #Parseamos primero para ver si puede ser una fecha
        try:
            dt = dateutil.parser.parse(x)
            #Es una fecha parseable
            cad = 'TO_DATE(' + comillas(x) + ',\'YYYY-MM-DD\')'
        except BaseException,e:
            if type(x) is str:
                cad = comillas(x)
            else:
                cad = str(x)
        cadena = cadena + cad 
        if argnumber < size:
            cadena = cadena + ','
	argnumber = argnumber + 1
    cadena = cadena + ')'
    return cadena


reason = Enum('NOTINLDAP','NOMANDATORY',"FAILARCHIVE","FAILDELETE","FAILARCHIVEDN","FAILDELETEDN",'UNKNOWN')

def formatReason(user,reason,attr,stats):
    stats.reason[reason._index] +=1
    return user+"\t"+reason._key+"\t"+attr

#CLASES
  
class Stats(object):
    "Clase para llevar las estadísticas de una sesion"
    def __init__(self,session):
        self.session = session
        self.total = 0
        self.correctos = 0
        self.failed = 0
        self.rollback = 0
        self.norollback = 0
        self.skipped = 0
        self.inicio = datetime.datetime.now()
        self.fin = None
        self.reason = [0] * len(reason)
        
    def show(self):
        Print(0,"-------------------------------")
        Print(0,"ESTADISTICAS DE LA SESION")
        Print(0,"-------------------------------")
        Print(0,"Total:\t\t",self.total)
        Print(0,"Correctos:\t",self.correctos)
        Print(0,"Incorrectos:\t",self.total - self.correctos)

        Print(0,"\n--- Detalles de fallos ---\n")
        Print(0,"Failed:\t\t",self.failed)
        Print(0,"Rollback:\t",self.rollback)
        Print(0,"Norollback:\t",self.norollback)
        Print(0,"Skip:\t\t",self.skipped)
        Print(0,"Suma:\t\t",self.failed+self.rollback+self.norollback+self.skipped)

        Print(0,"\n--- Razones del fallo ---\n")
        i = 0        
        for r in self.reason:
            Print(0,reason[i],":\t",r)
            i += 1
            
        Print(0,"\n--- Rendimiento ---\n")
        Print(0,"Inicio:\t\t",self.inicio.strftime('%d-%m-%y %H:%M:%S'))
        Print(0,"Fin\t\t",self.fin.strftime('%d-%m-%y %H:%M:%S'))
        elapsed = self.fin - self.inicio
        Print(0,"Elapsed:\t",elapsed)
        Print(0,"Rendimiento:\t",(self.total - self.skipped)/elapsed.seconds," users/sec")        

class Log(object):
    "Clase que proporciona acceso a los logs"
    def __init__(self,session):
        self.session = session
        self.fUsersDone = open(session.tardir+'/users.done','w')
        self.fUsersFailed = open(session.tardir+'/users.failed','w')
        self.fUsersRollback = open(session.tardir+'/users.rollback','w')
        self.fUsersNoRollback = open(session.tardir+'/users.norollback','w')
        self.fUsersSkipped = open(session.tardir+'/users.skipped','w')
        self.fLogfile = open(session.tardir+'/logfile','w')
        self.fUsersList = open(session.tardir+'/users.list','w')
        self.fBbddLog = open(session.tardir+'/bbddlog','w')
        self.fFailReason = open(session.tardir+'/failreason',"w")
        
    def writeDone(self,string):
        self.fUsersDone.writelines(string+"\n")
        self.fUsersDone.flush()
        self.session.stats.correctos += 1
        
    def writeFailed(self,string):
        self.fUsersFailed.writelines(string+"\n")
        self.fUsersFailed.flush()
        self.session.stats.failed += 1
        
    def writeFailReason(self,string):
        self.fFailReason.writelines(string+"\n")
        self.fFailReason.flush()
        
    def writeRollback(self,string):
        self.fUsersRollback.writelines(string+"\n")
        self.fUsersRollback.flush()
        self.session.stats.rollback += 1
        
    def writeNoRollback(self,string):
        self.fUsersNoRollback.writelines(string+"\n")
        self.fUsersNoRollback.flush()
        self.session.stats.norollback += 1
        
    def writeSkipped(self,string):
        self.fUsersSkipped.writelines(string+"\n")
        self.fUsersSkipped.flush()
        self.session.stats.skipped +=1
        
    def writeLog(self,string,newline):
        trail = "\n" if newline else ""
        self.fLogfile.write(string + trail)
        self.fLogfile.flush()        
        
    def writeBbdd(self,string):
        try:
            self.fBbddLog.write(string+"\n")
            self.fBbddLog.flush()
        except IOError:
            print "I/O Error({0}) : {1}".format(e.errno, e.strerror)
        except ValueError:
            print "Error de valor"
        except AttributeError,e:
            print "Error de atributo: ",e            

    def writeIterable(self,fHandle,iterable):
        line = "\n".join(iterable)
        line = line+"\n"          
        fHandle.writelines(line)
        fHandle.flush()
        
class Session(object):
    
    def abort(self,severity):
        "Funcion que lleva el control sobre el proceso de abortar"

        if EXTRADEBUG: Debug("ABORTALWAYS ES: ",ABORTALWAYS)
        if ABORTLIMIT == 0: 
            Print(0,'ABORT: No abortamos porque ABORTLIMIT es 0')
            return
                
        if ABORTALWAYS is True:
            Print(0,'ABORT: Error y ABORTALWAYS es True')
            exit(False)

        if ABORTINSEVERITY is True and severity is True:
            Print(0,'ABORT: Error con severidad y ABORTINSEVERITY es True')
            exit(False)
            
        self.abortCount = self.abortCount + 1
        if self.abortCount > self.abortLimit:
            Print(0,'ABORT: Alcanzada la cuenta de errores para abort')
            exit(False)
                    
        
    def die(self,user,rollback):
        "Funcion que controla si abortamos o no y gestiona los logs"
        if rollback:
            if user.rollback():
                if DEBUG: Debug("DEBUG-INFO: Rollback exitoso de ",user.cuenta)
                self.log.writeRollback(user.cuenta)
                self.abort(False)
            else:
                if DEBUG: Debug("DEBUG-WARNING: Rollback fallido de ",user.cuenta)
                self.log.writeNoRollback(user.cuenta)
                self.abort(True)
        else:
            self.log.writeFailed(user.cuenta)
        #Generamos y grabamos la razon del fallo
        if not user.failreason:
            self.log.writeFailReason(formatReason(user.cuenta,reason.UNKNOWN,"----",self.stats))
        else:
            self.log.writeFailReason(user.failreason)
        return False
            
    def __init__(self,sessionId,fromDate,toDate):
        config.session = self        
        self.sessionId = sessionId
        self.fromDate = fromDate
        self.toDate = toDate
        self.accountList = []
        self.userList = []
        self.tarsizes = 0
        self.tardir = ''
        self.abortCount = 0
        self.abortLimit = ABORTLIMIT
        self.abortDecrease = ABORTDECREASE
        self.abortAlways = ABORTALWAYS
        self.abortInSeverity = ABORTINSEVERITY
        self.maxSize = MAXSIZE
        self.idsesion = 0
        #Comprobamos los parametros para poder ejecutar
        if not self.sessionId: raise ValueError
        if not self.fromDate: self.fromDate = '1900-01-01'
        if not self.toDate: self.toDate = '2100-01-01'
        #Directorio para los tars
        if os.path.exists(config.TARDIR):            
            if self.sessionId:            
                self.tardir = config.TARDIR + '/' + self.sessionId
            if not os.path.isdir(self.tardir):
                os.mkdir(self.tardir,0777)
        else:
            #Abortamos porque no existe el directorio padre de los tars
            Print(0,'ABORT: (session-start) No existe el directorio para tars: ',config.TARDIR)
        self.log = Log(self)
        self.stats = Stats(self)
        Print(0,'Procesando la sesion ',self.sessionId,' desde ',self.fromDate,' hasta ',self.toDate)
        
    def getaccountList(self):
        if TEST:
            self.accountList = ['games','news','uucp','pepe']
        else:
            self.accountList = getListByDate(self.toDate,self.fromDate)
            
    def bbddInsert(self):
        """ Inserta un registro de archivado en la BBDD """
        now = datetime.datetime.now()
        try:
            cursor = oracleCon.cursor()
            #Consigo el idsesion
            self.idsesion = int(cursor.callfunc('UF_ST_SESION',cx_Oracle.NUMBER))
            values = valueslist(self.idsesion,now.strftime('%Y-%m-%d'),self.fromDate,self.toDate,self.sessionId)
            query = Q_INSERT_SESION + values
            self.log.writeBbdd(query)
            if DRYRUN and not SOFTRUN:
                return True
            cursor.execute(query)
            oracleCon.commit()
            cursor.close()    
            return True
        except BaseException,e:
            Print(0,"ERROR: Almacenando en la BBDD sesion ",self.sessionId)
            if DEBUG: Debug("DEBUG-ERROR: (sesion.bbddInsert) Error: ",e)
            return False
    
    def start(self):
        #Directorio para TARS
        if DEBUG: Debug('DEBUG-INFO: (session.start) config.TARDIR es: ',config.TARDIR)
        print "VERBOSE: ",VERBOSE,"DEBUG: ",DEBUG,"PROGRESS: ",PROGRESS
        if haveprogress(): pbar=ProgressBar(widgets=[Percentage()," ",Bar(marker=RotatingMarker())," ",ETA()],maxval=1000000).start()
        #Creo la lista de cuentas
        if not self.accountList:
            self.getaccountList()
        self.log.writeIterable(self.log.fUsersList,self.accountList)
        self.stats.total = len(self.accountList)
        if haveprogress(): pbar.update(100000)
        #Creo la lista de objetos usuario a partir de la lista de cuentas            
        if not self.userList:
            for account in self.accountList:
                user = User(account,self)
                if user.exclude:
                    self.log.writeFailed(user.cuenta)
                    #Generamos y grabamos la razon del fallo
                    self.log.writeFailReason(formatReason(user.cuenta,reason.NOTINLDAP,"----",self.stats))
                else:
                    self.userList.append(user) 
        if haveprogress(): pbar.update(200000)
        #Insertamos sesion en BBDD
        self.bbddInsert()
        #Proceso las entradas
        skip = False
        pp = 200000
        ip = 800000/len(self.userList)
        for user in self.userList:
            if haveprogress(): 
                pp = pp + ip                
                pbar.update(pp)
            if skip:
                self.log.writeSkipped(user.cuenta)
                continue
            #Chequeamos ...
            Print(1,"*** PROCESANDO USUARIO ",user.cuenta," ***")
            if not user.check():
                if not self.die(user,False):continue
            #... Archivamos ...
            if not user.archive(self.tardir):
                if not self.die(user,True): continue
            self.tarsizes = self.tarsizes + user.tarsizes
            #... Borramos storage ...
            if not user.deleteStorage():
                if not self.die(user,True): continue
            #Lo siguiente solo lo hacemos si tiene cuenta windows
            if 'WINDOWS' in user.cuentas:
                #... Almacenamos el DN ...
                if not user.archiveDN(self.tardir):
                    if not self.die(user,True): continue
                #... y borramos el DN            
                if not user.deleteDN():
                    if not self.die(user,True):
                        user.borraCuentaWindows()                        
                        continue
            #Escribimos el registro de usuario archivado
            if not DRYRUN:
                if not user.insertArchiveRecord():
                    if not self.die(user,True): continue
            #Si hemos llegado aquí todo esta OK
            if ABORTDECREASE: self.abortCount = self.abortCount -1
            if self.abortCount < 0: self.abortCount = 0
            if DEBUG: 
                Debug('DEBUG-INFO: (session.start) abortCount: '+str(self.abortCount))               
            self.log.writeDone(user.cuenta)
            #Controlamos si hemos llegado al tamaño maximo
            if MAXSIZE > 0:
                if self.tarsizes > MAXSIZE:
                    skip = True
        Print(1,'Tamaño de tars de la session ',self.sessionId,' es ',sizeToHuman(self.tarsizes))
        self.stats.fin = datetime.datetime.now()
        self.stats.show()
        
class Storage(object):
    
    def __init__(self,key,path,link,mandatory,parent):
        self.key = key
        self.path = path
        self.link = link
        self.mandatory = mandatory 
        self.tarpath = None
        self.parent = parent
        self.tarsize = 0
        self.state = state.NA
        self.accesible = None
        
    def display(self):
        Print(1,self.key,"\t = ",self.path,"\t Accesible: ",self.accesible,"\t Estado: ",self.state)

    def archive(self,rootpath):
        """ Archiva un storage en un tar"""
        #Vuelvo a comprobar aqui que es accesible
        if not self.accesible:
            self.state = state.NOACCESIBLE
            return False
        self.tarpath = rootpath + '/' + self.parent.cuenta + '_' + self.key + '_' + sessionId + ".tar"
        Print(1,"Archivando ",self.key," from ",self.path," in ",self.tarpath," ... ")
        try:
            if DRYRUN and not SOFTRUN: 
                #Calculo el tamaño sin comprimir y creo un fichero vacio para la simulacion                
                f = open(self.tarpath,"w")
                f.close()
                self.tarsize = os.lstat(self.tarpath).st_size                
                self.state = state.ARCHIVED                
                self.bbddInsert()
                return True
            tar = tarfile.open(self.tarpath,"w:bz2")
            tar.add(self.path)
            tar.close()
            self.tarsize = os.path.getsize(self.tarpath)
            self.state = state.ARCHIVED
            self.bbddInsert()
            return True
        except:
            Print(0,"ERROR: Archivando",self.key)
            self.state = state.TARFAIL
            return False
            
    def bbddInsert(self):
        """ Inserta un registro de archivado en la BBDD """
        try:
            #Como en sigu 
            values = valueslist(config.session.idsesion,self.parent.cuenta,self.tarpath,self.tarsize,self.state._index)
            query = Q_INSERT_STORAGE + values        
            config.session.log.writeBbdd(query)
            if DRYRUN and not SOFTRUN:
                return True
            cursor = oracleCon.cursor()
            cursor.execute(query)
            oracleCon.commit()
            cursor.close()                 
            return True
        except BaseException,e:
            Print(0,"ERROR: Almacenando en la BBDD storage ",self.key)
            if DEBUG: Debug("DEBUG-ERROR: (storage.bbddInsert) Error: ",e)
            return False
            
    def delete(self):
        "Borra un storage"
        #Primero tengo que controlar si no existe y no es mandatory
        if DEBUG: Debug("DEBUG-INFO: (storage.delete) ",self.key," en ",self.path)
        if not self.accesible and not self.mandatory:
            self.state = state.NOACCESIBLE
            return True
        try:        
            if DRYRUN: 
                self.state = state.DELETED                
                return True
            rmtree(self.path)
            if self.link is not None:
                os.remove(self.link)
            self.state = state.DELETED
            return True
        except:
            self.state = state.DELETEERROR
            return False
                
    def rollback(self):
        """Deshace el archivado borra los tars.
        - Si se ha borrado hacemos un untar
        - Borramos el tar
        - Ponemos el state como rollback"""
        if DEBUG: Debug('DEBUG-INFO: (storage.rollback)',self.__dict__)
        if self.state == state.DELETED or self.state == state.DELETEERROR:
            if not self.unarchive():
                self.state = state.ERROR
                return False
            #Restauro el link si existe
            if self.link is not None:
                if not DRYRUN:
                    os.link(self.link,self.path)
        try:
            #Si no está archivado no hay que borrar el tar
            if self.state not in (state.ARCHIVED,state.TARFAIL): 
                if DEBUG: Debug("DEBUG-INFO: (storage.rollback) No hago rollback de ",self.key," no estaba archivado")
                return True
            #Borramos el tar
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
                if DRYRUN: return True                
                tar = tarfile.open(self.tarpath,"r:*")
                tar.extractall(self.path)
                tar.close()
                return True
            except:
                Print(0,"Error unarchiving ",self.key," to ",self.path," from ",self.tarpath," ... ")
                return False

    def exist(self):
        """Comprueba la accesibilidad de un storage
        se tiene en cuenta que si no existe en el sitio por defecto
        puede existir en los root alternativos"""
        #Existe el path
        if DEBUG: Debug("DEBUG-INFO: ****** PROCESANDO ",self.path," *******")
        if os.path.exists(self.path):
            self.accesible = True
            return True
        #Aun no existiendo puede estar en un directorio movido, lo buscamos
        parentdir = os.path.dirname(self.path)
        basename = os.path.basename(self.path)
        #Nos aseguramos de que si ya hemos buscado y no hay alternativos salir
        if parentdir in config.altdirs and not config.altdirs[parentdir]:
            if DEBUG: Debug("DEBUG-WARNING: User:",self.parent.cuenta," No existe path directo ni alternativo para ",self.key," en ",parentdir)
            self.accesible = False
            return False
        if DEBUG: Debug("DEBUG-INFO: User:",self.parent.cuenta," No existe path directo para ",self.key," en ",self.path," busco alternativo ...")
        #Buscamos en directorios alternativos del parentdir
        #esta busqueda puede ser gravosa si se debe repetir para cada usuario por
        #lo que una vez averiguados los alternativos se deben de almacenar globalmente
        if not parentdir in config.altdirs: 
            if DEBUG: Debug("DEBUG-INFO: No he construido aun la lista alternativa para ",self.key," en ",parentdir," lo hago ahora ...")
            config.altdirs[parentdir] = [s for s in os.listdir(parentdir) 
                                        if s.startswith(ALTROOTPREFIX)] 
        #Si la lista esta vacia salimos directamente
        if not config.altdirs[parentdir]:
            if DEBUG: Debug("DEBUG-WARNING: No existen directorios alternativos para ",self.key," en ",parentdir)
            self.accesible = False
            return False
        #Buscamos si existe en cualquiera de los directorios alternativos
        for path in config.altdirs[parentdir]:
            joinpath = os.path.join(parentdir,path,basename)
            if os.path.exists(joinpath):
                if DEBUG: Debug("DEBUG-INFO: User:",self.parent.cuenta," encontrado alternativo para ",self.key," en ",joinpath)
                self.path = joinpath
                self.accesible = True
                return True
        #Si llegamos aqui es que no existe
        self.accesible = False
        return False
        
class User(object):
    global status    
    instancias = {}
    def __new__(cls,name,parent):
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
            if not storage.exist() and storage.mandatory:
                status = False
                self.failreason = formatReason(self.cuenta,reason.NOMANDATORY,storage.key,self.parent.stats)
                if DEBUG: Debug("DEBUG-WARNING: (user.check) El usuario ",self.cuenta," no ha pasado el chequeo")
                break
        return status
        
    def insertArchiveRecord(self):
        """Esta funcion es dummy pues con los datos de sigu se puede inferir si
        un usuario esta archivado o no"""
        return True
        
    def borraCuentaWindows(self):
        "Borra la cuenta windows de la BBDD sigu"
        return True
        
    def listCuentas(self):
        """Devuelve una tupla con las cuentas que tiene el usuario
        Por defecto tenemos correo y linux, para ver si tenemos windows
        consultamos si existe en la tabla UT_CUENTAS_NT, en AD o en ambos"""
        
        if TEST:                
            return ('LINUX','MAIL') #dummy return
        
        if hasCuentaNT(self.cuenta):
            return ("LINUX","MAIL","WINDOWS")
        else:
            return ("LINUX","MAIL")
            
    def __init__(self,cuenta,parent):
        try:
            dummy = self.cuenta
            if DEBUG: Debug("DEBUG-WARNING: (user.__init__) YA EXISTIA USUARIO ",self.cuenta, " VUELVO DE INIT")
            return
        except:
            pass
        self.parent = parent        
        self.exclude = False        
        self.dn = None
        self.adObject = None
        self.failreason = None
        self.cuenta = cuenta
        if TEST:
            self.homedir = cuenta
        else:
            try:            
                self.homedir = os.path.basename(ldapFromSigu(cuenta,'homedirectory'))
            except BaseException,e:
                self.failreason = reason.NOTINLDAP
                self.exclude = True
                return
        self.storage = []
        self.rootpath = ''
        self.cuentas = self.listCuentas()
        paseporaqui = False
        for c in self.cuentas:
            #relleno el diccionario storage
            for m in MOUNTS:
                sto_link = None
                #Si el montaje no esta paso a la siguiente cuenta
                if m['val'] is None:
                    continue
                if c == m['account']:
                    sto_path = m['val'] + '/' + self.homedir
                    sto_key = m['fs']
                    #Si es un enlace lo sustituyo por el path real
                    if os.path.islink(sto_path):
                        sto_link = sto_path
                        sto_path = os.path.realpath(sto_path)
                    #Caso especial de WINDOWS (calcular dn)
                    if c == 'WINDOWS':
                        #En el caso de windows el homedir es siempre la cuenta
                        sto_path = m['val'] + '/' + self.cuenta
                        status,dn,tupla,result_type = dnFromUser(self.cuenta)
                        if status:
                            self.dn = dn
                        else:
                            Print(0,"El usuario ",self.cuenta,"no tiene DN en AD y debería tenerla")
                            self.dn = False                       
                        if DEBUG and not paseporaqui: 
                            Debug("DEBUG-INFO: Usuario: ",self.cuenta," DN: ",self.dn)
                            paseporaqui = True
                    storage = Storage(sto_key,sto_path,sto_link,m['mandatory'],self)
                    self.storage.append(storage)
        #Rellenamos el dn
        if self.dn:        
            status,self.dn,self.adObject,result_type = dnFromUser(self.cuenta)

    def deleteStorage(self):
        "Borra todos los storages del usuario"
        for storage in self.storage:
            if storage.delete():
                continue
            else:
                if DEBUG: Debug("DEBUG-ERROR: (user.deleteStorage) user:",self.cuenta," Storage: ",storage.key)
                self.failreason = formatReason(self.cuenta,reason.FAILDELETE,storage.key,self.parent.stats)
                return False
        return True
        
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
            if not storage.rollback():
                return False
        return True
        
    def getRootpath(self,tardir):
        if not os.path.isdir(tardir):
            Print(0,"ABORT: (user-archive) No existe el directorio para TARS",tardir)
            exit(False)

        self.rootpath = tardir + '/' + self.cuenta
        if not os.path.isdir(self.rootpath):
            os.mkdir(self.rootpath,0777)    

    def unarchive(self,tardir):
        "Este metodo es useless pues debe rellenar los storages primero"
        #Vemos si rootpath existe
        if not self.rootpath: self.getRootpath(tardir)

        for storage in self.storage:
            storage.unarchive()
            
            
    def archive(self,tardir):
        self.tarsizes = 0
        "Metodo que archiva todos los storages del usuario"
        #Vemos si rootpath existe
        if not self.rootpath: self.getRootpath(tardir)
        
        for storage in self.storage:
            if not storage.archive(self.rootpath) and storage.mandatory: 
                if DEBUG: Debug("DEBUG-INFO: (user.archive) mandatory de ",storage.key," es ",storage.mandatory)
                self.failreason = formatReason(self.cuenta,reason.FAILARCHIVE,storage.key,self.parent.stats)
                ret = False
                break
            else:
                ret = True
                self.tarsizes = self.tarsizes + storage.tarsize
        if not ret:
            Print(0,'WARNING: Error archivando usuario ',self.cuenta,' fs ',storage.key,' haciendo rollback')
            self.rollback()
            try:
                 #Borramos el directorio padre
                 os.rmdir(self.rootpath)
            except:
                Print(0,'ABORT: No puedo borrar tar rootpath para ',self.cuenta,' ... abortando')
                exit(False)
        else:            
            Print(2,'INFO: El tamaño de los tars para ',self.cuenta,' es: ',self.tarsizes)
        return True

    def archiveDN(self,tardir):
        "Usando pickle archiva el objeto DN de AD"
        #Vemos si rootpath existe
        if not self.rootpath: self.getRootpath(tardir)
        if not self.adObject: return False
        try:
            adFile = open(self.rootpath+'/'+self.cuenta+'.dn','w')
            pickle.dump(self.adObject,adFile)
            adFile.close()
            return True
        except BaseException,e:
            self.failreason = formatReason(self.cuenta,reason.FAILARCHIVEDN,self.dn,self.parent.stats)
            return False
            
    def deleteDN(self):
        "Borra el dn del usuario"
        import ldap
        Print(1,'Borrando DN: ',self.dn)
        if self.dn is not None:
            try:            
                if DRYRUN: return True
                ldapCon.delete_s(self.dn)                
                return True
            except ldap.LDAPError, e:
                Print(0,"Error borrando DN usuario ",self.cuenta," ",e)
                self.failreason = formatReason(self.cuenta,reason.FAILDELETEDN,self.dn,self.parent.stats)
        return False
    
    def insertDN(self,tardir):
        "Como no es posible recuperar el SID no tiene sentido usarla"
        import ldap 
        
        if not self.rootpath: self.getRootpath(tardir)        

        if self.dn is None:
            try:
                adFile = open(self.rootpath+'/'+self.cuenta+'.dn','r')
                self.adObject = pickle.load(adFile)
                
                item = self.adObject[0]
                dn = item[0]
                atributos = item[1]
                if DEBUG: Debug("DEBUG-INFO: (user.insertDN) DN:",dn)
                if DEBUG: Debug("DEBUG-INFO: (user.insertDN) AT:",atributos)

                attrs=[]
                attrList = [ "cn", "countryCode", "objectClass", "userPrincipalName", "info", "name", "displayName", "givenName", "sAMAccountName" ]
                for attr in atributos:
                   if attr in attrList:
                      attrs.append( (attr, atributos[attr]))

                if DEBUG: Debug("DEBUG-INFO: (user.insertDN) ==== ATTRS ====",attrs)
                if not DRYRUN:                
                    ldapCon.add_s( dn, attrs)
                adFile.close()
                return True
            except ldap.LDAPError, e:
                Print(0,e)
                return False
        

import cmd        
class shell(cmd.Cmd):
    def parse(self,line):
        return line.split()
    def do_count(self,line):
        """
        Devuelve el numero de usuarios entre dos fechas
        <count fromDate toDate>
        """
        try:
            fromDate,toDate = self.parse(line)
            global WINDOWS_PASS
            WINDOWS_PASS = "dummy"
            CheckEnvironment()
            userlist = getListByDate(toDate,fromDate)
            print "Usuarios entre ",fromDate," y ",toDate," = ",len(userlist)
        except BaseException,e:
            print "Error recuperando la cuenta de usuarios: ",e

    def do_isarchived(self,line):
        """
        Devuelve si una cuenta tiene estado archivado
        <isarchived usuario>
        """
        try:
            global WINDOWS_PASS
            WINDOWS_PASS = "dummy"
            CheckEnvironment()
            print isArchived(line)
        except BaseException,e:
            print "Error recuperando el estado de archivado",e
      
    def do_hascuentant(self,line):
        """
        Comprueba si un usuario tiene cuenta NT
        <hascuentant usuario>
        """
        try:
            global WINDOWS_PASS
            global ORACLE_PASS
            if NTCHECK == 'ad': ORACLE_PASS = "dummy"
            if NTCHECK == 'sigu': WINDOWS_PASS = 'dummy'
            CheckEnvironment()
            print hasCuentaNT(line)," (metodo ",NTCHECK,")"
        except BaseException,e:
            print "Error comprobando si ",line," tiene cuenta NT",e  
            
    def do_ldapquery(self,line):
        """
        Consulta un atributo de ldap para una cuenta dada
        <ldapquery usuario atributo>
        """
        try:
            global WINDOWS_PASS
            WINDOWS_PASS = "dummy"
            CheckEnvironment()
            user,attr = self.parse(line)
            ret = ldapFromSigu(user,attr)
            print ret
        except BaseException,e:
            print "Error consultando atributo ldap",e  
            
    def do_quit(self,line):
        print "Hasta luego Lucas ...."
        exit(True)
        
    def __init__(self):
        cmd.Cmd.__init__(self)
        

"""
Programa principal
"""
import argparse

parser = argparse.ArgumentParser(description='Siguclean 0.1: Utilidad para borrar storages de usuarios',formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-n','--sessionname',help='Nombre de sesion',dest='sessionId',action='store',default=None)
parser.add_argument('-p','--altrootprefix',help='Prefijo para carpetas alternativas',dest='ALTROOTPREFIX',action='store',default=None)
parser.add_argument('-i','--interactive',help='Iniciar sesion interativa',action='store_true')
parser.add_argument('-a','--abortalways',help='Abortar siempre ante un error inesperado',dest='ABORTALWAYS',action='store_true',default='False')
parser.add_argument('-d','--abortdecrease',help='Decrementar la cuenta de errores cuando se produzca un exito en el archivado',dest='ABORTDECREASE',action='store_true',default='False')
parser.add_argument('-s','--abortinseverity',help='Abortar si se produce un error con severidad',dest='ABORTINSEVERITY',action='store_true',default='False')
parser.add_argument('-l','--abortlimit',help='Limite de la cuenta de errores para abortar (0 para no abortar nunca)',dest='ABORTLIMIT',action='store',default='5')
parser.add_argument('-f','--from',help='Seleccionar usuarios desde esta fecha',dest='fromDate',action='store',default=None)
parser.add_argument('-t','--to',help='Seleccionar usuarios hasta esta fecha',dest='toDate',action='store',default=None)
parser.add_argument('-m','--maxsize',help='Limite de tamaño del archivado (0 sin limite)',dest='MAXSIZE',action='store',default='0')
parser.add_argument('-w','--windows-check',help='Metodo de comprobacion de existencia de cuenta windows',choices=['ad','sigu','both'],dest='NTCHECK',action='store',default='ad')
parser.add_argument('--win-password',help='Clave del administrador de windows',dest='WINDOWS_PASS',action='store',default=None)
parser.add_argument('--sigu-password',help='Clave del usuario sigu',dest='ORACLE_PASS',action='store',default=None)
parser.add_argument('--test',help='Para usar solo en el peirodo de pruebas',dest='TEST',action='store_true')
parser.add_argument('--debug',help='Imprimir mensajes de depuracion',dest='DEBUG',action='store_true')
parser.add_argument('--dry-run',help='No realiza ninguna operacion de escritura',dest='DRYRUN',action='store_true')
parser.add_argument('--soft-run',help='Junto a dry-run, si genera los tars y la insercion en la BBDD',dest='SOFTRUN',action='store_true')
parser.add_argument('-v','--verbosity',help='Incrementa el detalle de los mensajes',action='count')
parser.add_argument('--progress',help='Muestra indicacion del progreso',dest='PROGRESS',action='store_true')
parser.add_argument('-x','--mount-exlude',help='Excluye esta regex de los posibles montajes',dest='MOUNT_EXCLUDE',action='store',default="(?=a)b")
parser.add_argument('--confirm',help='Pide confirmación antes de realizar determinadas acciones',dest='CONFIRM',action='store_true')

args = parser.parse_args()

VERBOSE = args.verbosity
if DEBUG: Debug('verbose es: ',VERBOSE)


#Si no es interactiva ponemos los valores a las globales
for var in args.__dict__:
    if var in globals().keys():
        if vars(args)[var] is not None:
            if args.DEBUG: Debug('DEBUG-INFO: existe ',var,' y es ',vars(args)[var])
            globals()[var] = vars(args)[var]
    
if args.interactive:
    shell().cmdloop()
    exit(0)

if DEBUG: Debug('DEBUG-INFO: sessionId: ',sessionId,'fromdate: ',fromDate,' todate: ',toDate,' abortalways: ',ABORTALWAYS,' verbose ',VERBOSE)

    
try:
    sesion = Session(sessionId,fromDate,toDate)
except BaseException,e:
    Print(0,'ABORT: No se ha dado nombre a la sesion')
    print "ERROR: ",e
    exit(False)
CheckEnvironment()
sesion.start()



    
    

     
