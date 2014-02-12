#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
#from __future__ import print_function

#Defines (globales)
TEST = False
ONESHOT = False
DEBUG = False
CHECKED = False
EXTRADEBUG = True
VERBOSE = 1
DRYRUN = False
SOFTRUN = False
CONFIRM = False
PROGRESS = False
IGNOREARCHIVED = False
FROMFILE = None
MAXSIZE = 0
RESTORE = False
RESTORING = False
TARDIR = None
EXCLUDEUSERSFILE = None
CONSOLIDATE = False
NUMSCHASERVICES = 5

#SERVICIOS
OFFSERVICES = {'L01': 'N', 'L02': 'N', 'L03': 'N', 'L04': 'N', 'L05': 'N'}

#VARIABLES DE CONFIGURACION
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
Q_GET_CUENTA_NT = 'SELECT CCUENTA FROM UT_CUENTAS_NT WHERE CCUENTA ='
Q_INSERT_STORAGE = 'INSERT INTO UT_ST_STORAGE (IDSESION,CCUENTA,TTAR,NSIZE,CESTADO,NSIZE_ORIGINAL,NFICHEROS) VALUES '
Q_INSERT_SESION = 'INSERT INTO UT_ST_SESION (IDSESION,FSESION,FINICIAL,FFINAL,DSESION) VALUES '
Q_IGNORE_ARCHIVED ='UF_ST_ULTIMA(CCUENTA) !=\'0\''
Q_ONLY_ARCHIVED = 'UF_ST_ULTIMA(CCUENTA) =\'0\''

#LDAP_SERVER = "ldap://ldap1.priv.uco.es"
LDAP_SERVER = "ldaps://ucoapp09.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
ORACLE_SERVER='ibmblade47/av10g'
ALTROOTPREFIX = '0_'
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
fAllOutput = None

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
import subprocess

state = Enum('NA','ARCHIVED','DELETED','TARFAIL','NOACCESIBLE','ROLLBACK','ERROR','DELETEERROR','UNARCHIVED','NOTARCHIVABLE')

#FUNCIONES

    
def fetchsingle(cursor):
    ret = cursor.fetchone()
    if len(ret) == 1:
        ret = ret[0]
    return ret

import contextlib
@contextlib.contextmanager
def cd_change(tmp):
    """Funcion para cambiar temporalmente a un directorio y volver al anterior despues"""
    cd = os.getcwd()
    os.chdir(tmp)
    try:
        yield
    finally:
        os.chdir(cd)
        
def haveprogress():
    """Comprueba si se dan las condiciones de mostrar la barra de progreso"""
    
    if PROGRESS and VERBOSE == 0: 
        return True
    else:
        return False
        
def confirm():
    """Pide confirmacion por teclado"""
    
    a = raw_input("Desea continuar S/N (N)")
    if a == "S":
        return True
    else:
        exit(True)
        
def iterable(obj):
    """Comprueba si un objeto es iterable"""
    
    if isinstance(obj, collections.Iterable):
        return True
    return False
    
def Pprint(*args):
    """Imprime de forma bonita algo, teniendo en cuenta si es iterable o no"""
    
    for arg in args:
        if iterable(arg): 
            pprint(arg),
        else:
            print(arg),

def CheckEnvironment():
    """Chequea el entorno de ejecucion (instancia unica)"""
    
    global CHECKED
    Print(1,"PASO1: Comprobando el entorno de ejecucion ...")
    if not CHECKED:
        CheckModules()
        CheckConnections()
        CheckMounts()
    CHECKED = True
    
    
def CheckModules():
    """Comprueba que son importables los módulos ldap y cx_Oracle"""
    
    Print(1,"  Comprobando modulos necesarios")

    #python_ldap
    Print(1,'     comprobando modulo conexion a ldap  ... ',end=' ')    
    try:
        global ldap
        import ldap
        Print(1,"CORRECTO")
    except:
        Print(0,"ABORT: No existe el modulo python-ldap, instalelo")
        os._exit(False)

    #cx_Oracle
    Print(1,'     comprobando modulo conexion a Oracle ... ',end='')    
    try:
        global cx_Oracle        
        import cx_Oracle
        Print(1,"CORRECTO")
    except:
        Print('ABORT: No existe el modulo cx_Oracle, instalelo')
        os._exit(False)
    
def openLdap(reconnect):
    if reconnect:
        verb = "DEBUG-WARNING: Reabriendo"
    else:
        verb = "Abriendo"

    if DEBUG: Debug(verb," conexión a ldap")
    try:
        global ldapCon
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, 0)
        ldap.set_option(ldap.OPT_REFERRALS,0)
        ldapCon = ldap.initialize(LDAP_SERVER)
        ldapCon.simple_bind_s(BIND_DN, WINDOWS_PASS)
        config.status.ldapCon = True
    except ldap.LDAPError, e:
        Print(1,"ERROR: ",verb," conexión a ldap")
        Print(e)
        config.status.ldapCon = False    

def CheckConnections():
    """Establece las conexiones a ldap y oracle"""
    
    Print(1,"  Comprobando conexiones")
    import ldap,cx_Oracle
    #LDAP
    global WINDOWS_PASS
    if not WINDOWS_PASS:
        WINDOWS_PASS = raw_input('     Introduzca la clave de windows (administrador): ')
    if WINDOWS_PASS != "dummy":
        Print(1,'     comprobando conexion a ldap ... ',end='')
        openLdap(False)
        if config.status.ldapCon is True:
            Print(1,"CORRECTO")

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
    """Devuelve el punto de montaje que contiene algo en el export"""
    
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
    """Comprueba que los puntos de montaje están accesibles"""
    
    Print(1,"  Comprobando el acceso a los Datos")
    try:
        regex = re.compile(MOUNT_EXCLUDE)
        if DEBUG: Debug("DEBUG-INFO: Regex de exclusion es ",MOUNT_EXCLUDE," y su valor es ",regex)
    except:
        Print(0,"ABORT: La expresion ",MOUNT_EXCLUDE," no es una regex valida, abortamos ...")
        os._exit(False)
    salgo = False
    for var in MOUNTS:
        Print(2,'     comprobando '+var['fs']+' ...',end='')
        var['val'] = get_mount_point(var['label'],regex)
        if var['val'] != None:
            Print(2,"Usando montaje ",var['val'])
            exec("config.status.%s = True" % (var['fs']))
        else:
            exec("config.status.%s = False" % (var['fs']))
            Print(2,"NO ACCESIBLE")
            salgo = True
    if salgo:
        Print(0,'ABORT: Algunos puntos de montaje no estan accesibles')
        os._exit(False)
    #Resumen de montajes
    if CONFIRM: 
        VERBLEVEL=0
    else:
        VERBLEVEL=1
    Print(VERBLEVEL,"*** RESUMEN DE MONTAJES ***")
    for var in MOUNTS:
        if len(var['label']) < 8:
            tabs = "\t\t\t"
        else:
            tabs = "\t\t"
        Print(VERBLEVEL,var['label'],tabs,var['val'])
    if CONFIRM: confirm()

def inputParameter(param,text,mandatory):
    """Lee un parametro admitiendo que la tecla intro ponga el anterior"""
    
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
    """Lee por teclado los parametros de ejecucion"""
    
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
    """Funcion paginador"""
    
    import itertools 
    args = [iter(iterable)] * page_size
    fillvalue = object()
    for group in itertools.izip_longest(fillvalue=fillvalue, *args):
        yield (elem for elem in group if elem is not fillvalue)  

def imprime(userList):        
    """Imprime usando un paginador"""
    
    my_pager = pager(userList,20)
    for page in my_pager:
        for i in page:
            Print(1,i)
        tecla = raw_input("----- Pulse intro para continuar (q para salir) ----------")
        if tecla == 'q':
            break       
        
def sizeToHuman(size):
    """Convierte un numero de bytes a formato humano"""
    
    symbols = ('B','K','M','G','T')
    indice = 0
    #if EXTRADEBUG: Debug("EXTRADEBUG-INFO: (sizeToHuman) Size antes de redondear es: ",size )
    while(True):
        if size < 1024:
            string = str(round(size,1))+" "+symbols[indice]
            return string
        size = size / 1024.0
        indice = indice + 1
    
def humanToSize(size):
    """Convierte un tamaño en formato humano a bytes"""
    
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
    """Devuelve un timestamp"""
    
    return '['+str(datetime.datetime.now())+']\t'
    
def Print(level,*args,**kwargs):
    """Formatea y archiva los mensajes por pantalla"""
    
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
            config.session.log.writeAllOutput(cadena+trail,False)
            
        
def Debug(*args,**kwargs):
    """Formatea y archiva los mensajes de debug"""
    
    global fDebug
    #Si tenemos verbose o no tenemos sesion sacamos la info por consola tambien
    if VERBOSE>0 or not config.session:
        print "".join(str(x) for x in args)
    #Si tenemos definido el log de la sesion lo grabamos en el fichero, en caso
    #contrario solo salen por pantalla
    #En sesiones restore no abrimos el fichero
    if config.session and not RESTORE:
        if not fDebug:
            fDebug = open(config.session.logsdir+"/debug","w")
            
        if kwargs != {}:
            trail = kwargs['end']
        else:
            trail = '\n'
        fDebug.write(timeStamp())
        if fAllOutput is not None: fAllOutput.write(timeStamp())
        for string in args:
            fDebug.write(str(string))
            if fAllOutput is not None: fAllOutput.write(str(string))
        fDebug.write(trail)
        if fAllOutput is not None: fAllOutput.write(trail)
        fDebug.flush()
        if fAllOutput is not None: fAllOutput.flush()
            
def dnFromUser(user):
    """Devuelve la DN de un usuario de active directory"""
    
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
    """Consulta un atributo ldap mediante sigu"""
    
    Q_LDAP_SIGU = 'select sigu.ldap.uf_leeldap(\''+cuenta+'\',\''+attr+'\') from dual'
    
    cursor = oracleCon.cursor()
    cursor.execute(Q_LDAP_SIGU)     
    tmpList = cursor.fetchall()
    tmpList = tmpList[0][0]
    if DEBUG: Debug("DEBUG-INFO (ldapFromSigu): tmplist = ",tmpList)
    return tmpList.strip().split(':')[1].strip() if tmpList else None
    
def schaFromLdap(cuenta):
    "Devuelve un diccionario con los permisos de la cuenta"
    Q_LDAP_SIGU = 'select cServicio,tServicio,uf_valida_servicio_ldap(' + comillas(cuenta) + ',cServicio) from ut_servicios_mapa'
    #Q_LDAP_SIGU = 'select sigu.ldap.uf_leeldap(\''+cuenta+'\',\'schacuserstatus\') from dual'
    cursor = oracleCon.cursor()
    cursor.execute(Q_LDAP_SIGU)     
    tmpList = cursor.fetchall()
    #tmpList = str(tmpList[0][0]).replace(' schacuserstatus :','').split()
    #Convertimos en un diccionario
    try:
        #ret = dict([tuple(x.split(":")) for x in tmpList])
        ret = dict([(x[0],x[2]) for x in tmpList])
        return ret
    except:
        #Vemos si no ha devuelto nada
        if x == "None":
            return None
        else:
            return False
            
def isServicesOff(services):
    if(len(services) < NUMSCHASERVICES):
        return len(services)
    for i in services:
        if services[i] != 'N' and i in OFFSERVICES:
            return False
    return True
    
def allServicesOff(user):
    services = schaFromLdap(user)
    #Si no devuelve nada, no tiene ningun servicio a off
    if services is None:
        return False
    else:
        return(isServicesOff(services))
        
def checkServices(userlist):
    for user in userlist:
        try:        
            services = schaFromLdap(user)
            if services is None:
                Print(1,"INFO: Usuario ",user," tiene todos los servicios en ON")
                continue
            if services is False:
                Print(1,"ERROR: Consultando los servicios de ",user)
                continue
            ret = isServicesOff(services)
            if ret is False:
                Print(1,"INFO: Usuario ",user," no tiene todos los servicios en OFF")
            elif ret is not True:
                Print(1,"INFO: Usuario ",user," tiene menos servicios de los esperados a OFF")
        except BaseException,e:
            Print(0,"ERROR: Error desconocido consultando servicios del usuario ",user)
            Print(0,"ERRORCODE: ",e)
            
def getListByDate(toDate , fromDate='1900-01-01'):
    """Devuelve una lista de cuentas entre dos fechas"""
    
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES
    if IGNOREARCHIVED is True:
        Print(1,'INFO: Ignorando los ya archivados')
        query = query + ' AND ' + Q_IGNORE_ARCHIVED
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
    
def getArchivedByDate(toDate , fromDate='1900-01-01'):
    """Devuelve una lista de cuentas entre dos fechas"""
    
    Q_BETWEEN_DATES = 'FCADUCIDAD  BETWEEN to_date(\''+ fromDate +\
                       '\',\'yyyy-mm-dd\') AND to_date(\''+ toDate +\
                       '\',\'yyyy-mm-dd\')'
    query = Q_GET_BORRABLES + ' AND ' + Q_BETWEEN_DATES + ' AND ' + Q_ONLY_ARCHIVED
    if DEBUG: Debug("DEBUG-INFO: (getListByDate) Query:",query)
    try:
        cursor = oracleCon.cursor()
        cursor.execute(query)     
        tmpList = cursor.fetchall()
        cursor.close()
    except BaseException,e:
        Print(0,"ERROR: Error recuperando la lista de usuarios archivados")
        if DEBUG: Debug("DEBUG-ERROR: (getListByDate): ",e)
        return None
    #Convertimos para quitar tuplas
    userList = [x[0] for x in tmpList]        
    config.status.userList = True
    return userList
    
def isArchived(user):
    """Comprueba si un usuario esta archivado. De momento solo manejo el código de salida
    de la función de sigu y no la información adicional. La casuistica es segun la salida
    de UF_ST_ULTIMA
    - 0 (ya archivado) o None: Devolvemos True (no procesar)
    - 1 o 2 (hay que archivar): Devolvemos False
    - 9 (el estado no es caducado o cancelado): Devolvemos la cadena de estado"""
    
    try:
        cursor = oracleCon.cursor()
        #Llamo a la función uf_st_ultima
        ret = cursor.callfunc('UF_ST_ULTIMA',cx_Oracle.STRING,[user])
        cursor.close()    
        if ret == "0":
            return True
        else:
            if ret is None:
                #Devolvemos como archivado si el usuario no existe (para fromfile)
                return True
            if ret[0] == "9":
                #Si el estado es distinto de caducado o cancelado, devolvemos el propio estado
                return ret[1]
            #Si estamos aquí la salida es 1 o 2 y no esta archivado por tanto
            return False
    except BaseException,e:
        if DEBUG: Debug("DEBUG-ERROR: (isArchived) ",e)
        return None

def isExpired(user):
    """Comprueba si un usuario esta esxpirado (caducado o cancelado)"""
    try:
        cursor = oracleCon.cursor()
        cursor.execute("select CESTADO from ut_cuentas where CCUENTA = " + comillas(user)) 
        ret = fetchsingle(cursor)
    except BaseException,e:
        Print(0,"ERROR: Consultando estado archivable del usuario ",user)
        Print(0,"ERROR-CODE: ",e)
        return None

    cursor.close()
    if ret == "":
        return None
    if ret == '4' or ret == '6':
        return True
    else:
        return False
    
def hasArchivedData(user):
    """Devuelve True si hay algun registro de archivado en ut_st_storage"""
    try:
        cursor = oracleCon.cursor()
        cursor.execute("select unique ccuenta from ut_st_storage where ccuenta = "+comillas(user))
        ret = fetchsingle(cursor)
        if ret == "":
            return False
        else:
            return True
    except:
        return False
        
def hasCuentaNT(cuenta):
    """Comprueba si un usuario tiene cuenta NT"""
    
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
    """ Nos devuelve una cadena entre comillas"""
    
    return '\''+cadena+'\''

def valueslist(*args,**kwargs):
    """Devuelve una cadena con una serie de valores formateados para sentencia sql"""
    
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


reason = Enum('NOTINLDAP','NOMANDATORY',"FAILARCHIVE","FAILDELETE","FAILARCHIVEDN","FAILDELETEDN",'UNKNOWN',"ISARCHIVED","UNKNOWNARCHIVED","NODNINAD","EXPLICITEXCLUDED","INSERTBBDDSTORAGE","NOTALLSERVICESOFF")

def formatReason(user,reason,attr,stats):
    """Formatea la razon de fallo devolviendo una cadena"""
    
    stats.reason[reason._index] +=1
    return user+"\t"+reason._key+"\t"+attr

def filterArchived(userlist):
    """Filtra de una lista de usuarios dejando solo los que no estan archivados
    descontando los que dan otro tipo de salida que no sea False"""
    
    userlist[:] = [ x for x in userlist if isArchived(x) is False]

def fromFile(userlist):
    """Lee la lista desde un fichero, teniendo en cuenta el filtro de exclusión"""
    
    if os.path.exists(FROMFILE):
        try:
            f = open(FROMFILE,"r")
            #Leemos los usuarios quitando el \n final
            userlist.extend([line.strip() for line in f])
            f.close()
            #Si tenemos IGNOREARCHIVED filtramos la lista
            if IGNOREARCHIVED is True:
                filterArchived(userlist)
                if EXTRADEBUG: Debug("EXTRADEBUG-INFO: Lista filtrada: ",userlist)
            return True
        except BaseException,e:
            if DEBUG: Debug("Error leyendo FROMFILE: ",e)
            Print(0,"Error leyendo FROMFILE: ",FROMFILE)
            return False   
    else:
        Print(0,"El fichero FROMFILE ",FROMFILE," no existe")
        return False    

def uniqueName(filename):
    """Devuelve un nombre unico apra un fichero que se va a renombrar"""
    contador = 0
    while os.path.exists(filename+"."+str(contador)):
        contador = contador + 1
    return filename+"."+str(contador)
            
#CLASES
  
class Stats(object):
    "Clase para llevar las estadísticas de una sesion"
    def __init__(self,session):
        self.session = session
        self.total = 0
        self.correctos = 0
        self.failed = 0
        self.excluded = 0
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
        Print(0,"Excluded:\t",self.excluded)
        Print(0,"Skip:\t\t",self.skipped)
        Print(0,"Suma:\t\t",self.failed+self.rollback+self.norollback+self.skipped+self.excluded)

        Print(0,"\n--- Razones del fallo/exclusion ---\n")
        i = 0        
        for r in self.reason:
            Print(0,reason[i],":\t",r)
            i += 1
            
        Print(0,"\n--- Rendimiento ---\n")
        Print(0,"Inicio:\t\t",self.inicio.strftime('%d-%m-%y %H:%M:%S'))
        Print(0,"Fin\t\t",self.fin.strftime('%d-%m-%y %H:%M:%S'))
        elapsed = self.fin - self.inicio
        Print(0,"Elapsed:\t",elapsed)
        #El cálculo del rendimiento lo escalamos
        _users = self.total - self.skipped
        if _users > elapsed.seconds:
            _rendimiento = _users/elapsed.seconds
            Print(0,"Rendimiento:\t",_rendimiento," users/sec")        
        else:
            _rendimiento = (_users * 60)/elapsed.seconds
            if _rendimiento >=1:
                Print(0,"Rendimiento:\t",_rendimiento," users/min")    
            else:
                _rendimiento = elapsed.seconds/_users
                Print(0,"Rendimiento:\t",_rendimiento," sec/user") 

class Log(object):
    "Clase que proporciona acceso a los logs"
    def __init__(self,session):
        global fAllOutput
        self.session = session
        #Creamos el directorio logs si no existe. Si existe renombramos el anterior
        if CONSOLIDATE:
            session.logsdir = session.tardir+"/consolidatelogs"
        else:
            session.logsdir = session.tardir+"/logs"
        if not os.path.exists(session.logsdir):
            os.mkdir(session.logsdir,0777)
        else:
            #Tenemos que tener en cuenta de si es una sesion restore
            #caso de no serla rotamos el log. 
            #si  lo es, usamos el mismo log solo para ller cmdline ya que el fork lo rotara
            if not RESTORE:            
                newname = uniqueName(session.logsdir)
                os.rename(session.logsdir,newname)
                os.mkdir(session.logsdir,0777)
        #Si es restore salimos sin crear fichero ninguno
        if RESTORE:
            return
        #Abrimos todos los ficheros
        #if not CONSOLIDATE:
        self.fUsersDone = open(session.logsdir+'/users.done','w')
        self.fUsersFailed = open(session.logsdir+'/users.failed','w')
        self.fUsersRollback = open(session.logsdir+'/users.rollback','w')
        self.fUsersNoRollback = open(session.logsdir+'/users.norollback','w')
        self.fUsersSkipped = open(session.logsdir+'/users.skipped','w')
        self.fUsersExcluded = open(session.logsdir+'/users.excluded','w')
        self.fUsersList = open(session.logsdir+'/users.list','w')
        self.fFailReason = open(session.logsdir+'/failreason',"w")

        self.fLogfile = open(session.logsdir+'/logfile','w')
        self.fBbddLog = open(session.logsdir+'/bbddlog','w')
        self.fAllOutput = open(session.logsdir+'/alloutput',"w")
        fAllOutput = self.fAllOutput
        self.fCreateDone = open(session.logsdir+'/create.done',"w")
        self.fRenameDone = open(session.logsdir+'/rename.done',"w")
        self.fRenameFailed = open(session.logsdir+'/rename.failed',"w")

    def writeCreateDone(self,string):
        self.fCreateDone.writelines(string+"\n")
        self.fCreateDone.flush()

    def writeRenameDone(self,string):
        self.fRenameDone.writelines(string+"\n")
        self.fRenameDone.flush()

    def writeRenameFailed(self,string):
        self.fRenameFailed.writelines(string+"\n")
        self.fRenameFailed.flush()
            
    def writeDone(self,string):
        self.fUsersDone.writelines(string+"\n")
        self.fUsersDone.flush()
        self.session.stats.correctos += 1
        
    def writeFailed(self,string):
        self.fUsersFailed.writelines(string+"\n")
        self.fUsersFailed.flush()
        self.session.stats.failed += 1
        
    def writeExcluded(self,string):
        self.fUsersExcluded.writelines(string+"\n")
        self.fUsersExcluded.flush()
        self.session.stats.excluded += 1
        
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
        #En sesiones restore no escribimos el log
        if not RESTORE:
            trail = "\n" if newline else ""
            self.fLogfile.write(string + trail)
            self.fLogfile.flush()        
        
    def writeAllOutput(self,string,newline):
        #En sesiones restore no escribimos el log
        if not RESTORE:
            trail = "\n" if newline else ""
            self.fAllOutput.write(string + trail)
            self.fAllOutput.flush()        

    def writeBbdd(self,string):
        try:
            self.fBbddLog.write(string+"\n")
            self.fBbddLog.flush()
        except IOError,e:
            Print(0,"ERROR: (writeBbdd)", "I/O Error({0}) : {1}".format(e.errno, e.strerror))
        except ValueError,e:
            Print(0,"ERROR: (writeBbdd)","Error de valor: ",e)
        except AttributeError,e:
            Print(0,"ERROR: (writeBbdd)","Error de atributo: ",e)            

    def writeIterable(self,fHandle,iterable):
        line = "\n".join(iterable)
        line = line+"\n"          
        fHandle.writelines(line)
        fHandle.flush()
        
class Session(object):
    "Clase para procesar una sesion de archivado"

    def __init__(self,sessionId,fromDate,toDate):
        global MAXSIZE
        config.session = self        
        self.sessionId = sessionId
        self.fromDate = fromDate
        self.toDate = toDate
        self.accountList = []
        self.userList = []
        self.excludeuserslist = []
        self.tarsizes = 0
        self.tardir = ''
        self.abortCount = 0
        self.abortLimit = ABORTLIMIT
        self.abortDecrease = ABORTDECREASE
        self.abortAlways = ABORTALWAYS
        self.abortInSeverity = ABORTINSEVERITY
        self.idsesion = None
        self.logsdir = ''

        #Comprobamos los parametros para poder ejecutar
        if not self.sessionId: raise ValueError
        if not self.fromDate: self.fromDate = '1900-01-01'
        if not self.toDate: self.toDate = '2100-01-01'
        #Comprobamos que existe TARDIR
        if TARDIR is None:
            raise Exception("No ha dado valor a sessiondir")
        #Directorio para los tars
        if os.path.exists(TARDIR):
            if self.sessionId:            
                self.tardir = TARDIR + '/' + self.sessionId
            if not os.path.isdir(self.tardir):
                os.mkdir(self.tardir,0777)
        else:
            #Abortamos porque no existe el directorio padre de los tars
            Print(0,'ABORT: (session-start) No existe el directorio para tars: ',TARDIR)
            os._exit(False)
        self.log = Log(self)
        self.stats = Stats(self)
        #Tratamos MAXSIZE
        #Intentamos convertir MAXSIZE a entero
        try:
            a = int(MAXSIZE)
            MAXSIZE = a
            if DEBUG is True: Debug("MAXSIZE era un entero y vale ",MAXSIZE)
        except BaseException,e:
            #Es una cadena vemos si es auto, convertible de humano o devolvemos error        
            if MAXSIZE == "auto":
                try:
                    statfs = os.statvfs(TARDIR)
                    MAXSIZE = int(statfs.f_bsize * statfs.f_bfree * 0.9)
                    if DEBUG: Debug("MAXSIZE era auto y vale ",MAXSIZE)
                except BaseException,e:
                    Print(0,"ABORT: Calculando MAXSIZE para ",TARDIR)
                    os._exit(False)
            else:
                a = humanToSize(MAXSIZE)
                if a is not False:
                    MAXSIZE = a
                    if DEBUG: Debug("MAXSIZE era sizehuman y vale ",MAXSIZE)
                else:
                    Print(0,"ABORT: opción MAXSIZE invalida: ",MAXSIZE)
                    os._exit(False)

        #Tratamos el fichero EXCLUDEUSERSFILE
        if EXCLUDEUSERSFILE is not None:
            Print(0,"Excluyendo usuarios de ",EXCLUDEUSERSFILE)
            if os.path.exists(EXCLUDEUSERSFILE):
                try:
                    f = open(EXCLUDEUSERSFILE,"r")
                    #Leemos los usuarios quitando el \n final
                    self.excludeuserslist.extend([line.strip() for line in f])
                    f.close()
                except BaseException,e:
                    if DEBUG: Debug("Error leyendo EXCLUDEUSERSFILE: ",e)
                    Print(0,"Error leyendo EXCLUDEUSERSFILE: ",FROMFILE)
                    return False                  
            else:
                Print(0,"ABORT: No exite el fichero de exclusion de usuarios")
                os._exit(False)

        Print(0,'Procesando la sesion ',self.sessionId,' desde ',self.fromDate,' hasta ',self.toDate)
 
    
    def accountlistFromCurrent(self):
        "Lee userlist en base a los usuarios archivados previamente"
        #Comprobamos que no existe ya para sesiones que se hayan creado sin procesar.        
        if not self.accountList:
            try:
                self.accountList = [s for s in os.listdir(self.tardir) if (not s.startswith("logs") and not s.startswith("consolidatelogs") and os.path.isdir(self.tardir + "/" + s))]
                return True
            except BaseException,e:
                Print(0,"ABORT: No puedo recuperar la lista de usuarios previamente archivados")
                Print(0,"ERROR: ",e)
                return False
        
    def getSessionId(self):
        "Devuelve el ID de la BBDD en base a la descripcion de sesion"
        QGETIDSESION = "SELECT IDSESION FROM UT_ST_SESION WHERE DSESION = "
        try:
            cursor = oracleCon.cursor()
            cursor.execute(QGETIDSESION + comillas(self.sessionId))     
            self.idsesion = fetchsingle(cursor)
            cursor.close()
        except BaseException,e:
             Print(0,"ERROR: Recuperando id de sesion ",self.sessionId)
             Print(0,"ERROR: ",e)
             return False
        return self.idsesion

    def logdict(self,logsdirs,pathname):
        from collections import defaultdict            

        tmpdict = defaultdict(list)
        lineas = 0
        for logsdir in logsdirs:
            for line in open(self.tardir + "/" + logsdir + "/" + pathname).readlines():
                tmpdict[line].append(None)
                lineas = lineas +1
        return tmpdict,lineas
        
    def consolidateLogs(self):
        "Consolida los logs de una sesión con múltiples logs"
        
        cPath = self.tardir + "/consolidatelogs"
        if os.path.exists(cPath):
            Print(0,"ABORT: Los logs ya están consolidados")
            return

        logsdirs = [s for s in os.listdir(self.tardir) if s.startswith("logs")]   
        
        if len(logsdirs) == 1:
            Print(0,"ABORT: Solo hay una carpeta de logs, no es necesario consolidar")
            return

        usersdonedict,lineas = self.logdict(logsdirs,'users.done')
        print "Lineas: ", lineas," DoneDict: ",len(usersdonedict)
        usersfaileddict,lineas = self.logdict(logsdirs,'users.failed')
        print "Lineas: ", lineas," FailedDict: ",len(usersfaileddict)
        userslistdict,lineas = self.logdict(logsdirs,'users.list')
        print "Lineas: ", lineas," ListDict: ",len(userslistdict)
        usersrollbackdict,lineas = self.logdict(logsdirs,'users.rollback')
        print "Lineas: ", lineas," RollbackDict: ",len(usersrollbackdict)
        
        ppp = set(usersrollbackdict).difference(set(usersdonedict))
        print "DifRollbackLen: ",len(ppp)

        return
        
    def consolidateFs(self,fs):
        import glob
        "Consolida un FS de una sesion previa"
        Print(1,"\n**** CONSOLIDANDO ",fs," ****\n")
        origindict = self.getOrigin(fs)
        Q_UPDATE = 'UPDATE UT_ST_STORAGE SET '
        Q_WHERE = " WHERE TTAR = "
        oneshot = False
        #Hay que tener en cuenta que en el diccionario puede haber más entradas que en
        #los archivados, pues aquellos que fallaron posteriormente SI generaron 
        #el log sobre el que nos hemos basado para averiguar los orígenes.
        #Por tanto recorreremos el FS y usaremos el dict para consultar el nuevo nombre
        
        archives = [s for s in os.listdir(self.tardir) if (not s.startswith("logs") and s != "consolidatelogs")]
        if DEBUG: Debug("DEBUG: Archives len es: ",len(archives))
        
        #Vemos la diferencia entre uno y otro, estos serán los que no se han procesado.
        diff = set(origindict.keys()).difference(set(archives))
        if DEBUG: Debug("DEBUG: La diferencia entre origenes y archives es: ",len(diff))
        #Abrimos el cursor
        try:
            cursor = oracleCon.cursor()
        except BaseException,e:
            Print(0,"ABORT: ConsolidateFs, Error abriendo cursor de oracle")
            os._exit(False)

        #Bucle de renombrado
        for archive in archives:
            path = self.tardir + "/" + archive + "/*_" + fs + "_*"
            try:            
                origen = glob.glob(path)[0]
            except:
                Print(0,"WARNING: El usuario: ",archive," no tiene ",fs)
                continue
            fich = origen
            fich = re.sub("_","@",fich)
            #Si la entrada de diccionario no es None            
            if origindict[archive]:
                destino = re.sub(fs,fs+"="+origindict[archive],fich)
            else:
                destino = fich

            #Parámetros para el update de BBDD de sigu
            setQ = "TTAR = " + comillas(destino)
            updateQ = Q_UPDATE + setQ + Q_WHERE + comillas(origen)    
            
            #renombrar el fichero
            try: 
                if not DRYRUN:
                    os.rename(origen,destino)
                    try:
                        self.log.writeBbdd(updateQ + "\n")
                        cursor.execute(updateQ)
                        self.log.writeRenameDone(origen+' '+destino)
                    except BaseException,e:
                        Print(0,"ERROR: Haciendo update, error: ",e," file: ",origen)
                        #deshago el renombrado
                        os.rename(destino,origen)
                        self.log.writeRenameFailed(origen+' '+destino)
                    if DEBUG:
                        Debug("DEBUG: Renombrando ",origen," ---> ",destino)
                else:
                    #TEST: Para probar en pruebas el renombrado con dryrun
                    #os.rename(origen,destino)
                    self.log.writeBbdd(updateQ + "\n")
                    Print(0,"INFO: Renombrando ",origen," ---> ",destino)
                    Print(0,"INFO: UpdateQ= ",updateQ)
                    self.log.writeRenameDone(origen+' '+destino)
            except BaseException,e:
                Print(0,"ERROR: Renombrando ",origen," a ",destino)
                Print(0,"ERROR: ",e)
            
            if not oneshot and ONESHOT: 
                confirm()
                oneshot = True
                
        if not DRYRUN:
            cursor.close()                            
            oracleCon.commit()

        return True

    def consolidate(self,fslist):
        """Consolida una sesión. Le pasaremos una lista de los label de los fs a 
        procesar"""
        #Consolidamos los FS
        if fslist is not None:
            for fs in fslist:
                if not self.consolidateFs(fs):
                    Print(0,"ABORT: Consolidando fs ",fs)
                    os._exit(False)
                    
        #Consolidamos los logs
        if not self.consolidateLogs():
            Print(0,"ABORT: Consolidando los logs")
            os._exit(False)

        return True
    
    def checkServices(self):
        archives = [s for s in os.listdir(self.tardir) if (not s.startswith("logs") and s != "consolidatelogs")]
        checkServices(archives)        
        
    def getOrigin(self,fs):
        "Recupera la carpeta origen de un determinado archivado"
        #Debo recorrer los ficheros logfile de todos los directorios logs
        #de la sesión, quedándome con las líneas que contienen "Archivando <storage>"
        #, extrayendo la cuarta columna y procesando esta para quitarle la primera parte que
        #es la raiz, despues tengo el alternativo (o el natural) y por ultimo el usuario
        global DEBUG
        from collections import defaultdict
        origindict = defaultdict(str)
        
        #Buscamos en mounts la base
        regex = re.compile(MOUNT_EXCLUDE)
        #Desactivamos temporalmente DEBUG
        tmpDEBUG = DEBUG
        DEBUG = False
        for mount in MOUNTS:
            if mount['fs'] == fs:
                raiz = get_mount_point(mount['label'],regex)
        #Recuperamos el valor de DEBUG
        DEBUG = tmpDEBUG
        
        logsdirs = [s for s in os.listdir(self.tardir) if s.startswith("logs")] 
        
        for logsdir in logsdirs:
            for line in open(self.tardir+"/"+logsdir+"/logfile").readlines():
                if line.startswith("Archivando "+fs):
                    dummy,dummy,dummy,result,dummy = line.split(None,4)
                    try:
                        dummy,origin,user = result.replace(raiz,'').split('/')
                    except ValueError:
                        #Si no puede desempaquetar es porque no se hizo de un alternativo
                        origin = None                        
                        dummy,user = result.replace(raiz,'').split('/')
                    origindict[user] = origin
        #En este punto ya tenemos un diccionario con las entradas únicas de 
        #los origenes sacados de los logs                    
        return origindict
                
    def abort(self,severity):
        "Funcion que lleva el control sobre el proceso de abortar"

        if EXTRADEBUG: Debug("EXTRADEBUG-INFO: ABORTALWAYS ES: ",ABORTALWAYS)
        if ABORTLIMIT == 0: 
            Print(0,'ABORT: No abortamos porque ABORTLIMIT es 0')
            return
                
        if ABORTALWAYS is True:
            Print(0,'ABORT: Error y ABORTALWAYS es True')
            os._exit(False)

        if ABORTINSEVERITY is True and severity is True:
            Print(0,'ABORT: Error con severidad y ABORTINSEVERITY es True')
            os._exit(False)
            
        self.abortCount = self.abortCount + 1
        if self.abortCount > self.abortLimit:
            Print(0,'ABORT: Alcanzada la cuenta de errores para abort')
            os._exit(False)
                    
        
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
            if user.exclude:
                self.log.writeExcluded(user.cuenta)                
            else:
                self.log.writeFailed(user.cuenta)
        #Generamos y grabamos la razon del fallo
        if not user.failreason:
            self.log.writeFailReason(formatReason(user.cuenta,reason.UNKNOWN,"----",self.stats))
        else:
            self.log.writeFailReason(user.failreason)
        return False
            
       
    def getaccountList(self):
        if TEST:
            self.accountList = ['games','news','uucp','pepe']
            return True
        else:
            if FROMFILE is not None:
                #Leo la lista de usuarios de FROMFILE
                return fromFile(self.accountList)
            else:
                #Recupero la lista de usuarios de SIGU
                self.accountList = getListByDate(self.toDate,self.fromDate)
                return True
            
    def bbddInsert(self):
        """ Inserta un registro de archivado en la BBDD """
        now = datetime.datetime.now()
        #Distinguimos entre sesiones restoring y normales.
        #Para las normales generamos un nuevo indice.
        #Para las restoring usamos el previamente almacenado
        if Debug: Debug("DEBUG-INFO: (session-bbddinsert) RESTORING es: ",RESTORING)
        if not RESTORING and not CONSOLIDATE:        
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
                #Salvamos en idsesion para restoring
                if DEBUG: Debug("DEBUG-INFO: (session-bbddinsert) salvando idsesion: ",self.idsesion)
            except BaseException,e:
                Print(0,"ERROR: Almacenando en la BBDD sesion ",self.sessionId)
                if DEBUG: Debug("DEBUG-ERROR: (sesion.bbddInsert) Error: ",e)
                return False
        else:
            #Leemos el valor de la sesion en curso
            self.idsesion = int(self.getSessionId())
            if DEBUG: Debug("DEBUG-INFO: Leido ID sesion: ",self.idsesion)

        f = open(self.logsdir+"/idsesion","w")
        f.write(str(self.idsesion)+"\n")
        f.close()
        return True

    def start(self):
        #Directorio para TARS
        if DEBUG: Debug('DEBUG-INFO: (session.start) TARDIR es: ',TARDIR)
        print "VERBOSE: ",VERBOSE,"DEBUG: ",DEBUG,"PROGRESS: ",PROGRESS
        if haveprogress(): pbar=ProgressBar(widgets=[Percentage()," ",Bar(marker=RotatingMarker())," ",ETA()],maxval=1000000).start()
        if not CONSOLIDATE:
            #Creo la lista de cuentas
            if not self.accountList:
                ret = self.getaccountList()
            #Si ret es False ha fallado la recuperacion de la lista de cuentas
            if not ret:
                Print(0,"ABORT: No he podido recuperar la lista de usuarios. Abortamos ...")
                os._exit(False)
        #Si la lista esta vacia no hay nada que procesar y salimos inmediatamente
        if len(self.accountList) == 0:
            Print(0,"EXIT: La lista de usuarios a procesar es vacia")
            os._exit(True)
        #Comenzamos el procesamiento
        self.log.writeIterable(self.log.fUsersList,self.accountList)
        self.stats.total = len(self.accountList)
        if haveprogress(): pbar.update(100000)
        #Creo la lista de objetos usuario a partir de la lista de cuentas            
        pp = 100000
        ip = 100000/len(self.accountList)
        if not self.userList:
            for account in self.accountList:
                if haveprogress(): 
                    pp = pp + ip                
                    pbar.update(pp)
                user = User(account,self)
                #Manejamos la exclusion del usuario
                if user.exclude:
                    self.log.writeExcluded(user.cuenta)
                    #Generamos y grabamos la razon del fallo
                    self.log.writeFailReason(formatReason(user.cuenta,user.failreason,"----",self.stats))
                else:
                    self.userList.append(user) 
        if haveprogress(): pbar.update(200000)
        #Insertamos sesion en BBDD
        self.bbddInsert()
        #Proceso las entradas
        skip = False
        pp = 200000
        ip = 800000/len(self.userList)

        #Bucle principal de procesamiento de usuarios        
        for user in self.userList:
            #Salimos si hemos creado el fichero indicador de parada
            if os.path.exists(self.tardir+"/STOP"):
                os.remove(self.tardir+"/STOP")
                Print(0,"Abortado por el usuario con fichero STOP")
                os._exit(True)
            #Escribimos en user.current el usuario actual por si el programa
            #casca en medio del procesamiento y el usuario se queda a medio hacer
            f = open(self.logsdir+"/users.current","w")
            f.write(user.cuenta)
            f.close()
            
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
                Print(0,"ERROR: Archivando usuario ",user.cuenta)
                if not self.die(user,True): continue
            self.tarsizes = self.tarsizes + user.tarsizes
            #... Borramos storage ...
            if not user.deleteStorage():
                Print(0,"ERROR: Borrando storages de usuario ",user.cuenta)
                if not self.die(user,True): continue
            #Lo siguiente solo lo hacemos si tiene cuenta windows
            if 'WINDOWS' in user.cuentas and not CONSOLIDATE:
                #Si falla el archivado de DN continuamos pues quiere decir que no está en AD
                #Si ha hecho el archivado y falla el borrado, hacemos rollback
                if not user.archiveDN(self.tardir):
                    if DEBUG: Debug("DEBUG-WARNING: Error archivando DN de ",user.cuenta)
                    if not self.die(user,False): 
                        pass                        
                        #continue
                else: 
                    if not user.deleteDN():
                        if DEBUG: Debug("DEBUG-WARNING: Error borrando DN de ",user.cuenta)
                        if not self.die(user,True):
                            user.borraCuentaWindows()                        
                            continue
                            #continue
            #Escribimos el registro de usuario archivado
            if not user.bbddInsert():
                Print(0,"ERROR: Insertando storages en BBDD de usuario ",user.cuenta)
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
        self.originalkey = key
        self.path = path
        self.link = link
        self.tarpath = None
        self.parent = parent
        self.tarsize = 0
        self.state = state.NA
        self.accesible = None
        self.size_orig = 0
        self.files = 0
        self.morestoragelist = []
        self.directstorage = False
        self.secondary = False
        if CONSOLIDATE:
            self.mandatory = False
        else:
            self.mandatory = mandatory 

    def display(self):
        Print(1,self.key,"\t = ",self.path,"\t Accesible: ",self.accesible,"\t Estado: ",self.state)

    def archive(self,rootpath):
        """ Archiva un storage en un tar"""
        #Vuelvo a comprobar aqui que es accesible
        if not self.accesible:
            self.state = state.NOACCESIBLE
            return False
        self.tarpath = rootpath + '/' + self.parent.cuenta + '@' + self.key + '@' + sessionId + ".tar.bz2"
        Print(1,"Archivando ",self.key," from ",self.path," in ",self.tarpath," ... ")
        try:
            if DRYRUN and not SOFTRUN: 
                #Calculo el tamaño sin comprimir y creo un fichero vacio para la simulacion                
                f = open(self.tarpath,"w")
                config.session.log.writeCreateDone(self.tarpath)
                f.close()
                self.tarsize = os.lstat(self.tarpath).st_size                
                self.state = state.ARCHIVED                
                #self.bbddInsert()
                return True
            #Cambiamos temporalmente al directorio origen para que el tar sea relativo
            with cd_change(self.path):            
                tar = tarfile.open(self.tarpath,"w:bz2")
                tar.add(".")
                #Calculamos el tamaño original y el numero de ficheros
                members = tar.getmembers()
                for member in members:
                    self.size_orig = self.size_orig + member.size
                self.files = len(members)
                #Fin del calculo
                tar.close()
                config.session.log.writeCreateDone(self.tarpath)
            self.tarsize = os.path.getsize(self.tarpath)
            self.state = state.ARCHIVED
            #Muevo el almacenamiento en la BBDD al finaldel proceso del usuario para que sea mas transaccional            
            #self.bbddInsert()
            return True
        except BaseException,e:
            Print(0,"ERROR: Archivando ",self.key)
            if DEBUG: Debug("DEBUG-ERROR: ",e)
            self.state = state.TARFAIL
            return False
            
    def bbddInsert(self,cursor):
        """ Inserta un registro de archivado en la BBDD """
        #Solo procesamos si el storage se completo y por tanto esta en estado deleted
        if EXTRADEBUG: Debug("EXTRADEBUG-INFO: (user-bbddInsert) self.state: ",self.state," key: ",self.key)
        if self.state == state.DELETED:
            try:
                #Como en sigu 
                values = valueslist(config.session.idsesion,self.parent.cuenta,self.tarpath,self.tarsize,self.state._index,self.size_orig,self.files)
                query = Q_INSERT_STORAGE + values        
                config.session.log.writeBbdd(query)
                if DRYRUN and not SOFTRUN:
                    return True
                cursor.execute(query)
                #Hago el commit en el nivel superior            
                #oracleCon.commit()
                #cursor.close()                 
                return True
            except BaseException,e:
                Print(0,"ERROR: Almacenando en la BBDD storage ",self.key)
                if DEBUG: Debug("DEBUG-ERROR: (storage.bbddInsert) Error: ",e)
                return False
        else:
            #Si no estaba archivado y no cascó lo consideramos correcto
            return True
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
        except BaseException,e:
            if DEBUG: Debug("DEBUG_ERROR: Borrando ",self.path," : ",e)
            self.state = state.DELETEERROR
            return False
                
    def rollback(self):
        """Deshace el archivado borra los tars.
        - Si se ha borrado hacemos un untar
        - Borramos el tar
        - Ponemos el state como rollback"""
        if EXTRADEBUG: Debug('EXTRADEBUG-INFO: (storage.rollback)',self.__dict__)
        if self.state in(state.DELETED,state.DELETEERROR):
            if not self.unarchive():
                self.state = state.ERROR
                return False
            #Restauro el link si existe
            if self.link is not None:
                if not DRYRUN:
                    os.link(self.link,self.path)
        try:
            #Si no está archivado no hay que borrar el tar
            if self.state not in (state.ARCHIVED,state.TARFAIL,state.UNARCHIVED): 
                if DEBUG: Debug("DEBUG-INFO: (storage.rollback) No borro ",self.key," no estaba archivado, state = ",self.state)
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
        if self.state in (state.DELETED,state.ARCHIVED,state.DELETEERROR):
            try:
                Print(1,"Unarchiving ",self.key," to ",self.path," from ",self.tarpath," ... ")                
                if DRYRUN: return True                
                tar = tarfile.open(self.tarpath,"r:*")
                tar.extractall(self.path)
                tar.close()
                self.state = state.UNARCHIVED
                return True
            except:
                Print(0,"Error unarchiving ",self.key," to ",self.path," from ",self.tarpath," ... ")
                return False

    def oldexist(self):
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

    def accesiblenow(self):
        "Comprueba si esta accesible en este momento"
        if os.path.exists(self.path):
            self.accesible = True
            return True
        else:
            self.accesible = False
            return False
        

    def exist(self):
        """Construye el storage
        """
        #Buscamos todos los storages para ese FS
        parentdir = os.path.dirname(self.path)
        basename = os.path.basename(self.path)
        directpath = os.path.exists(self.path)        
        firstpath = True
 
        #Si estamos en una sesion de consolidacion es posible que el usuario
        #haya "revivido" desde que se archivo. Por tanto si se trata de la ubicacion
        #normal debemos saltarla.
        if directpath and CONSOLIDATE:
            if DEBUG: Debug("DEBUG-INFO: INCONSISTENCIA, el usuario ",self.parent.cuenta," ha resucitado, no proceso ",self.key)
            directpath = False

        #Como no hay path directo ni ubicaciones alternativas para este tipo de storage
        #salimos  de la funcion retornando false.
        if not directpath  and parentdir in config.altdirs and not config.altdirs[parentdir]:
            if DEBUG: Debug("DEBUG-WARNING: User:",self.parent.cuenta," No existe path directo ni alternativo para ",self.originalkey," en ",parentdir)
            self.accesible = False
            return False
        
        #Buscamos en directorios alternativos del parentdir
        #esta busqueda puede ser gravosa si se debe repetir para cada usuario por
        #lo que una vez averiguados los alternativos se deben de almacenar globalmente
        if not parentdir in config.altdirs: 
            if DEBUG: Debug("DEBUG-INFO: No he construido aun la lista alternativa para ",self.originalkey," en ",parentdir," lo hago ahora ...")
            config.altdirs[parentdir] = [s for s in os.listdir(parentdir) 
                                        if s.startswith(ALTROOTPREFIX)] 

        #Comprobamos el directo
        if directpath:
            if DEBUG: Debug("DEBUG-INFO: Encontrado path directo para ",self.originalkey)
            self.accesible = True
            self.directstorage = True
            firstpath = False
        
        #¿Existen ubicaciones alternativas?
        if config.altdirs[parentdir]:
            if DEBUG: Debug("DEBUG-INFO: Existen ubicaciones alternativas de ",self.originalkey, " cuenta: ",self.parent.cuenta)
        #Buscamos si existe en cualquiera de los directorios alternativos
            for path in config.altdirs[parentdir]:
                joinpath = os.path.join(parentdir,path,basename)
                if os.path.exists(joinpath):
                    if DEBUG: Debug("DEBUG-INFO: User:",self.parent.cuenta," encontrado alternativo para ",self.originalkey," en ",joinpath)
                    #Tenemos que discriminar si es el primero o no
                    if firstpath:
                        #El storage alternativo encontrado es el primero de la lista
                        #damos el cambiazo del directo, que no se va a procesar, por los
                        #atributos del alternativo
                        self.path = joinpath
                        self.key = self.originalkey + "=" + path
                        self.accesible = True
                        firstpath = False
                    else:
                        #Este alternativo no es el primero por lo que va a la lista
                        #morestoragelist
                        altstorage = Storage(self.originalkey+"="+path,joinpath,None,self.mandatory,self.parent)
                        altstorage.accesible = True
                        altstorage.secondary = True
                        self.morestoragelist.append(altstorage)
        else:
            if DEBUG: Debug("DEBUG-INFO: No existen ubicaciones alternativas de ",self.originalkey, " cuenta: ",self.parent.cuenta)
            
        
        if not self.morestoragelist:
            if DEBUG: Debug("DEBUG-INFO: No existen storages adicionales para ",self.parent.cuenta," de ",self.originalkey," en ",parentdir)

        if not self.accesible:
            return False
        else:
            return True
            
        
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
        y si el usuario fue previamente archivado o no
        Asumo que la DN está bien porque acabo de buscarla."""
        #es archivable? Si no tiene todos los servicios a off, aun caducado o cancelado no debemos procesarlo
        if not allServicesOff(self.cuenta):
            self.exclude = True
            if DEBUG: Debug("DEBUG-WARNING: (user.check) El usuario ",self.cuenta," no tiene todos los servicios a off")
            self.failreason = formatReason(self.cuenta,reason.NOTALLSERVICESOFF,"---",self.parent.stats)
            #Este usuario se va a excluir, pero en una sesion de consolidacion al ser esta comprobacion nueva
            #hay que tener en cuenta que ya se archivo algo. Se me dice que aunque asi fuera, que solo se marque la
            #razon de la exclusion y se deje lo demas como esta.   A mi no me convence porque deja flecos, pero bueno.          
            return False
        #Esta archivado?
        archived = isArchived(self.cuenta)
        #Si la sesion es de consolidacion pasamos del chequeo de si esta archivado o no
        if not CONSOLIDATE:
            if archived is True:
                status = False
                self.failreason = formatReason(self.cuenta,reason.ISARCHIVED,"---",self.parent.stats) 
                if DEBUG: Debug("DEBUG-WARNING: (user.check) El usuario ",self.cuenta," ya estaba archivado")
                self.exclude = True            
                return status
            elif archived is None:
                status = False
                self.failreason = formatReason(self.cuenta,reason.UNKNOWNARCHIVED,"---",self.parent.stats) 
                if DEBUG: Debug("DEBUG-ERROR: (user.check) Error al comprobar estado de archivado de ",self.cuenta)
                self.exclude = True            
                return status
            elif archived is not False:
                status = False
                self.failreason = formatReason(self.cuenta,reason.NOTARCHIVABLE,"---",archived) 
                if DEBUG: Debug("DEBUG-WARNING: (user.check) El usuario ",self.cuenta," no es archivable, estado de usuario: ",archived)
                self.exclude = True            
                return status
            
        #El usuario no esta archivado, compruebo sus storages            
        status = True
        for storage in self.storage:
            #Si es secundario ya esta procesado y pasamos de el.
            if storage.secondary:
                continue
            if not storage.exist() and storage.mandatory:
                status = False
                self.failreason = formatReason(self.cuenta,reason.NOMANDATORY,storage.key,self.parent.stats)
                if DEBUG: Debug("DEBUG-WARNING: (user.check) El usuario ",self.cuenta," no ha pasado el chequeo")
                break
            else:
                #Con el nuevo modelo, storage puede tener una lista de mas storages
                #así que lo proceso
                if storage.morestoragelist:
                    for st in storage.morestoragelist:
                        self.storage.append(st)

        return status
        
    def bbddInsert(self):
        """Archiva en la BBDD todos los storages de usuario archivados"""
        cursor = oracleCon.cursor()
        for storage in self.storage:
            ret = storage.bbddInsert(cursor)
            if not ret:
                #Debo hacer un rollback
                if DEBUG: Debug("DEBUG-ERROR: (user-bbddInsert) Insertando: ",storage.key)                
                oracleCon.rollback()
                cursor.close()
                self.failreason = formatReason(self.cuenta,reason.INSERTBBDDSTORAGE,storage.key,self.parent.stats)
                return False
        #Debo hacer un commit
        oracleCon.commit()
        cursor.close()
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
        #Compruebo si esta explicitamente excluido
        if self.cuenta in self.parent.excludeuserslist:
            self.failreason = reason.EXPLICITEXCLUDED
            self.exclude = True
            return
            
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
        Print(2,"*** ROLLBACK INIT *** ",self.cuenta)
        for storage in self.storage:
            if not storage.rollback():
                return False
        #Si llegamos aquí, borramos el directorio padre
        try:
            rmtree(self.rootpath)
        except BaseException,e:
            Print(0,"ABORT: Error borrando rootpath: ",self.rootpath," error: ",e)
            os._exit(False)
        Print(2,"*** ROLLBACK OK *** ",self.cuenta)
        return True
        
    def getRootpath(self,tardir):
        if not os.path.isdir(tardir):
            Print(0,"ABORT: (user-archive) No existe el directorio para TARS",tardir)
            os._exit(False)

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
            #Originalmente hacía aquí un rollback directamente y devolvía True.
            #Devuelvo False y gestiono el rollback en la función die            
            #self.rollback()
            return False
        else:            
            Print(2,'INFO: El tamaño de los tars para ',self.cuenta,' es: ',self.tarsizes)
            return True

    def archiveDN(self,tardir):
        "Usando pickle archiva el objeto DN de AD"
        #Vemos si rootpath existe
        if not self.rootpath: self.getRootpath(tardir)
        if not self.adObject: 
            self.failreason = formatReason(self.cuenta,reason.NODNINAD,"---",self.parent.stats)
            return False
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
        #Dependiendo del tiempo transcurrido en el archivado, puede haberse superado
        #el timeout de la conexión y haberse cerrado. En este caso se presentará la
        #excepción ldap.SERVER_DOWN. Deberemos reabrir la conexión y reintentar el borrado
        
        #import ldap
        Print(1,'Borrando DN: ',self.dn)
        global ldapCon

        if self.dn is not None:
            try:            
                if DRYRUN: return True
                ldapCon.delete_s(self.dn)                
                return True
            except ldap.SERVER_DOWN:
                #La conexión se ha cerrado por timeout, reabrimos
                openLdap(True) 
                if config.status.ldapCon is True:
                    try:
                        ldapCon.delete_s(self.dn)    
                        return True
                    except ldap.LDAPError, e:
                        Print(0,"Error borrando DN usuario despues de reconexion ",self.cuenta," ",e)
                        self.failreason = formatReason(self.cuenta,reason.FAILDELETEDN,self.dn,self.parent.stats)
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
        Si tenemos fromfile no hace falta meter las dos fechas
        """
        global WINDOWS_PASS
        WINDOWS_PASS = "dummy"
        CheckEnvironment()

        if FROMFILE is not None:
            userlist = []
            ret = fromFile(userlist)
            if not ret:
                print "Error recuperando la cuenta de usuarios de ",FROMFILE
            else:
                print "Usuarios de ",FROMFILE," archivables = ",len(userlist)
            return
        else:    
            try:
                fromDate,toDate = self.parse(line)
                userlist = getListByDate(toDate,fromDate)
            except BaseException,e:
                print "Error recuperando la cuenta de usuarios de SIGU: ",e
                return
            print "Usuarios archivables entre ",fromDate," y ",toDate," = ",len(userlist)

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
    
    def do_schacquery(self,line):
        """Consulta el atributo schacUserStatus de ldap
        schacquery <usuario>"""
        global WINDOWS_PASS
        WINDOWS_PASS = "dummy"
        CheckEnvironment()
        ret = schaFromLdap(line)
        print ret
        
    def do_allservicesoff(self,line):
        """Comprueba si todos los servicios de un usuario estan a off
        allservicesoff <usuario>"""
        global WINDOWS_PASS
        WINDOWS_PASS = "dummy"
        CheckEnvironment()
        print allServicesOff(line)        

    def do_advsarchived(self,line):
        """
        Lista aquellos archivados que aun estan en AD
        <advsarchived fromdate todate>
        En caso de no especificar las fechas, se tomam todo el rango temporal
        """
        CheckEnvironment()

        try:
            if line == '':
                fromDate = '1900-01-01'
                toDate = '2099-01-01'
            else:
                fromDate,toDate = self.parse(line)
            #userlist = getArchivedByDate(toDate,fromDate)
            userlist = getArchivedByDate(toDate,fromDate)
        except BaseException,e:
            print "Error recuperando lista de usuarios archivados de SIGU: ",e
            return

        global NTCHECK
        NTCHECK = 'ad'
        contador = 0
        for user in userlist:
            if hasCuentaNT(user):
                print user
                contador = contador + 1
        print "Usuarios archivados entre ",fromDate," y ",toDate," = ",len(userlist)
        print "Usuarios archivados que aun tienen cuenta AD: ",contador
            
    def do_archived(self,line):
        """
        Lista aquellos archivados entre dos fechas (basada en su caducidad en ut_cuentas)
        <archived fromdate todate>
        En caso de no especificar las fechas, se tomam todo el rango temporal
        """
        CheckEnvironment()

        try:
            if line == '':
                fromDate = '1900-01-01'
                toDate = '2099-01-01'
            else:
                fromDate,toDate = self.parse(line)
            #userlist = getArchivedByDate(toDate,fromDate)
            userlist = getArchivedByDate(toDate,fromDate)
        except BaseException,e:
            print "Error recuperando lista de usuarios archivados de SIGU: ",e
            return

        print "Usuarios archivados entre ",fromDate," y ",toDate," = ",len(userlist)
        for user in userlist:
            print user
            
    def do_isexpired(self,line):
        """ Muestra si un usuario esta expirado
        isexpired <usuario>"""
        CheckEnvironment()
        print isExpired(line)        
            
    def do_stats(self,line):
        """Devuelve estadisticas sobre el proceso de archivado
        stats"""
        CheckEnvironment()
        
        cursor = oracleCon.cursor()
        cursor.execute("select sum(nficheros) from ut_st_storage")
        nficheros = fetchsingle(cursor)
        cursor.execute("select sum(nsize_original) from ut_st_storage")
        nsize_original = fetchsingle(cursor)
        cursor.execute("select sum(nsize) from ut_st_storage")
        nsize = fetchsingle(cursor)
        cursor.execute("select count(*) from ut_st_storage")
        ntars = fetchsingle(cursor)
        cursor.execute("select count(*) from ut_st_sesion")
        nsesiones =fetchsingle(cursor)
        cursor.execute("select count(distinct(ccuenta)) from ut_st_storage")
        narchivados = fetchsingle(cursor)
        cursor.execute("select nficheros,ccuenta from ut_st_storage where nficheros = (select max(nficheros) from ut_st_storage)")
        maxficheros,cuenta_maxficheros = fetchsingle(cursor)
        cursor.execute("select nsize_original,ccuenta from ut_st_storage where nsize_original = (select max(nsize_original) from ut_st_storage)")
        maxsize_orig,cuenta_maxsize_orig = fetchsingle(cursor)
        cursor.close()
        
        print "*********************************"
        print "*** ESTADISTICAS DE SIGUCLEAN ***"
        print "*********************************"
        print "\n"
        print "Sesiones:\t",nsesiones
        print "Archivados:\t",narchivados
        print "Numero tars:\t",ntars
        print "Ficheros:\t",nficheros
        print "Tamaño Orig:\t",sizeToHuman(nsize_original)
        print "Tamaño Arch:\t",sizeToHuman(nsize)
        print "Max ficheros:\t",maxficheros,"(",cuenta_maxficheros,")"
        print "Max tamaño:\t",sizeToHuman(maxsize_orig),"(",cuenta_maxsize_orig,")"
        
    def do_arcinfo(self,line):
        """Muestra información de archivado del usuario.
           arcinfo usuario"""
        from texttable import Texttable
        CheckEnvironment()
        
        cursor = oracleCon.cursor()
        cursor.execute("select * from ut_st_storage where ccuenta = "+comillas(line))
        rows = cursor.fetchall()   

        if rows == []:
            print "Usuario no archivado"
        else:
            table = Texttable()
            table.add_row(["TARNAME","SIZE","ORIGSIZE","FILES"])
            for row in rows:
                table.add_row([os.path.basename(row[2]),sizeToHuman(row[3]),sizeToHuman(row[5]),row[6]])
            print table.draw()
    
    def do_sql(self,line):
        """Permite ejecutar una consulta sql directamente contra sigu
        sql <consulta>"""
        from texttable import Texttable
        CheckEnvironment()

        try:
            cursor = oracleCon.cursor()
            cursor.execute(line)
            rows = cursor.fetchall()
            col_names = []
            for i in range(0,len(cursor.description)):
                col_names.append(cursor.description[i][0])
        except BaseException,e:
            print "ERROR: ",e
            return
            
        if rows == []:
            print "No results"
        else:
            table = Texttable(0)
            table.header(col_names)
            #table.set_deco(Texttable.BORDER | Texttable.HLINES | Texttable.VLINES)
            table.add_rows(rows,header=False)
            print table.draw()        
    
    def do_ignorearchived(self,line):
        """Muestra o cambia si debe ignorar los usuarios ya archivados en la selección
            ignorearchived <True/False>"""

        import ast
        global IGNOREARCHIVED 
        if line == "":
            print IGNOREARCHIVED
        else:
            try:
                IGNOREARCHIVED = ast.literal_eval(line)
                print IGNOREARCHIVED
            except BaseException,e:
                print "Valor booleano incorrecto"
   
    def do_checkaltdir(self,line):
        """Chequea y ofrece estadisticas de directorios alt para un directorio raiz dado
        checkaltdir <directorio>"""
        from collections import defaultdict
        CheckEnvironment()
        dictdir = defaultdict(list)
        sumuserlist = 0
        parentdir = line+"/"
        altdirs = [s for s in os.listdir(parentdir) 
                            if s.startswith(ALTROOTPREFIX)] 

        for altdir in altdirs:
            userlist = os.listdir(parentdir+altdir)
            sumuserlist = sumuserlist + len(userlist)
            for user in userlist:
                dictdir[user].append(altdir)
                
        fm = open("/tmp/multi-movidos","w")
        fs = open("/tmp/single-movidos","w")

        for k,v in dictdir.iteritems():
            if len(v) > 1:
                f = fm
            else:
                f = fs
            
            f.write(k)
            if hasArchivedData(k):
                f.write("\t"+"_ARC_")
            else:
                f.write("\t"+"NOARC")
                
            for value in v:
                f.write("\t"+value)
            f.write("\n")
        f.close()
        print "LENDICT: ",len(dictdir)
        print "SUMLIST: ",sumuserlist
        
            
            
        
    def do_quit(self,line):
        print "Hasta luego Lucas ...."
        os._exit(True)
        
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
parser.add_argument('--ignore-archived',help='Excluye de la selección las cuentas con estado archivado',dest='IGNOREARCHIVED',action='store_true',default='False')
parser.add_argument('-m','--maxsize',help='Limite de tamaño del archivado (0 sin limite)',dest='MAXSIZE',action='store',default='0')
parser.add_argument('-w','--windows-check',help='Metodo de comprobacion de existencia de cuenta windows',choices=['ad','sigu','both'],dest='NTCHECK',action='store',default='sigu')
parser.add_argument('--win-password',help='Clave del administrador de windows',dest='WINDOWS_PASS',action='store',default=None)
parser.add_argument('--sigu-password',help='Clave del usuario sigu',dest='ORACLE_PASS',action='store',default=None)
parser.add_argument('--test',help='Para usar solo en el peirodo de pruebas',dest='TEST',action='store_true')
parser.add_argument('--debug',help='Imprimir mensajes de depuracion',dest='DEBUG',action='store_true')
parser.add_argument('--dry-run',help='No realiza ninguna operacion de escritura',dest='DRYRUN',action='store_true')
parser.add_argument('--soft-run',help='Junto a dry-run, si genera los tars y la insercion en la BBDD',dest='SOFTRUN',action='store_true')
parser.add_argument('-v',help='Incrementa el detalle de los mensajes',dest='verbosity',action='count')
parser.add_argument('--progress',help='Muestra indicacion del progreso',dest='PROGRESS',action='store_true')
parser.add_argument('-x','--mount-exlude',help='Excluye esta regex de los posibles montajes',dest='MOUNT_EXCLUDE',action='store',default="(?=a)b")
parser.add_argument('--confirm',help='Pide confirmación antes de realizar determinadas acciones',dest='CONFIRM',action='store_true')
parser.add_argument('--fromfile',help='Nombre de fichero de entrada con usuarios',dest='FROMFILE',action='store',default=None)
parser.add_argument('--sessiondir',help='Carpeta para almacenar la sesion',dest='TARDIR',action='store',default=None)
parser.add_argument('--restore',help='Restaura la sesion especificada',dest='RESTORE',action='store_true')
parser.add_argument('--restoring',help='Opcion interna para una sesion que esta restaurando una anterior. No usar.',dest='RESTORING',action='store',default=False)
parser.add_argument('--consolidate',help='Consolida la sesion especificada',dest='CONSOLIDATE',action='store_true')
parser.add_argument('--exclude-userfile',help='Excluir los usuarios del fichero parámetro',dest='EXCLUDEUSERSFILE',action='store',default=None)
args = parser.parse_args()

VERBOSE = args.verbosity
#NOTA: Los debugs previos a la creación de la sesión no se pueden almacenar en el fichero
#debug pues aun no hemos establecido la rotación de logs

if DEBUG and not RESTORE: Debug('verbose es: ',VERBOSE)

#Si no es interactiva ponemos los valores a las globales
for var in args.__dict__:
    if var in globals().keys():
        if vars(args)[var] is not None:
            if args.DEBUG: Debug('DEBUG-INFO: existe ',var,' y es ',vars(args)[var])
            globals()[var] = vars(args)[var]

if args.interactive:
    shell().cmdloop()
    os._exit(True)

if DEBUG and not RESTORE and not CONSOLIDATE: Debug('DEBUG-INFO: sessionId: ',sessionId,'fromdate: ',fromDate,' todate: ',toDate,' abortalways: ',ABORTALWAYS,' verbose ',VERBOSE)

cmdline = sys.argv
cmdlinestr = ' '.join(cmdline)
  
try:
    sesion = Session(sessionId,fromDate,toDate)
except BaseException,e:
    Print(0,'ABORT: Error en la creacion de la sesion')
    print "ERROR: ",e
    os._exit(False)

#Guardamos los argumentos
#Si no es una sesión restore salvamos el string
if not RESTORE:
    f = open(sesion.logsdir+"/cmdline","w")
    f.write(cmdlinestr+"\n")
    f.close()
else:
    #Leemos la linea de comando anterior y le añadimos --ignore-archived si no lo tenía
    #Siempre trabajaremos sobre la linea de comando original del directorio logs
    Print(0,"... Restaurando sesion anterior ...")
    #Leemos la linea de comando
    f = open(sesion.logsdir+"/cmdline","r")
    cmdlinestr = f.readline().rstrip('\n') 
    f.close()
    #Leemos el idsesion anterior
    f = open(sesion.logsdir+"/idsesion","r")
    oldidsesion = f.readline().rstrip('\n') 
    f.close()
    
    if not "--ignore-archived" in cmdlinestr:
        cmdlinestr = cmdlinestr + " --ignore-archived"
    if not " --restoring" in cmdlinestr:
        cmdlinestr = cmdlinestr + " --restoring " + oldidsesion
    #Lanzamos el subproceso
    p = subprocess.Popen(cmdlinestr,shell=True) 
    p.wait()
    os._exit(True)

CheckEnvironment()

if not CONSOLIDATE:
    sesion.start()
else:
    #Estamos en una sesion de consolidacion. Algunas comprobaciones previas
    Print(0,"... Consolidando sesion anterior ...")
    #Comprobamos que existe la sesion y leemos el id
    if not sesion.getSessionId():
        os._exit(False)
    if DEBUG: Debug("DEBUG-INFO: Idsession es ",sesion.idsesion)        
    #Generamos la lista de usuarios a partir de los ya archivados
    if not sesion.accountlistFromCurrent():
        os._exit(False)
    if DEBUG: Debug("DEBUG-INFO: Recuperada lista usuarios desde FS. Numero usuarios: ",len(sesion.userList))
    sesion.consolidateFs('homenfs')
    sesion.consolidateFs('homemail')
    sesion.consolidateFs('perfiles')
    sesion.consolidateFs('homecifs')
    sesion.start()
os._exit(True)



    
    

     
