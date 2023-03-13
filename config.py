# -*- coding: utf-8 -*-
"""
Created on Wed May 22 09:27:33 2013

@author: tonin
"""
from enum import Enum

class Mrelax(Enum):
    NONE = '1'
    CANCELADOS = '2'
    TODOS = '3'

class Cestado(Enum):
    CADUCADO = '4'
    CANCELADO = '6'

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

# Defines (globales)
__version__ = "1.1.3"
TEST = False
ONESHOT = False
DEBUG = False
EXTRADEBUG = False
COUNTDEBUG = False
CHECKED = False
VERBOSE = 1
DRYNOWRITE = False
DRYRUN = False
SOFTRUN = False
COUNTRUN = False
CONFIRM = False
PROGRESS = False
IGNOREARCHIVED = False
FROMFILE = None
MAXSIZE = 0
RESTORE = False
RESTORING = False
EXCLUDEUSERSFILE = None
CONSOLIDATE = False
NUMSCHASERVICES = 5
LDAPRELAX = False
CHECKARCHIVEDDATA = False
MANDATORYRELAX = Mrelax.NONE
TRACE = False
MANUALDELETE = False
CLOSEORACLEONLDAP = True
BYPASS = False

TOFILE = None
TOFILEHANDLE = None


# SERVICIOS
OFFSERVICES = {'L01': 'N', 'L02': 'N', 'L03': 'N', 'L04': 'N', 'L05': 'N'}

# VARIABLES DE CONFIGURACION
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
Q_GET_CUENTA_NT = 'SELECT CCUENTA FROM UT_CUENTAS_NT WHERE CCUENTA = %s'
Q_INSERT_STORAGE = 'INSERT INTO UT_ST_STORAGE (IDSESION,CCUENTA,TTAR,NSIZE,CESTADO,NSIZE_ORIGINAL,NFICHEROS) VALUES %s'
Q_INSERT_SESION = 'INSERT INTO UT_ST_SESION (IDSESION,FSESION,FINICIAL,FFINAL,DSESION) VALUES %s'
Q_IGNORE_ARCHIVED = 'UF_ST_ULTIMA_TONIN(CCUENTA) !=\'0\''
Q_ONLY_ARCHIVED = 'UF_ST_ULTIMA_TONIN(CCUENTA) =\'0\''

# LDAP_SERVER = "ldap://ldap1.priv.uco.es"
LDAP_SERVER = "ldaps://docad01.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
ORACLE_SERVER = 'ibmblade47/av10g'
ALTROOTPREFIX = '0_'
NTCHECK = 'ad'
TARDIR = None

# Claves
WINDOWS_PASS = None
ORACLE_PASS = None

# Control del abort
"""
ABORTLIMIT: Numero de fallos admitidos
ABORTDECREASE: Si un borrado Ok debe decrementar la cuenta de fallos
ABORTALWAYS: Si cualquier fallo debe abortar
ABORTINSEVERITY: Si un fallo con severidad debe abortar
"""
ABORTLIMIT = 5
ABORTDECREASE = True
ABORTALWAYS = False
ABORTINSEVERITY = False
fDebug = None
fAllOutput = None

# Filtro de exclusion de montajes, lo inicializamos como algo imposible de cumplir
MOUNT_EXCLUDE = "(?=a)b"

#state = Enum('NA', 'ARCHIVED', 'DELETED', 'TARFAIL', 'NOACCESIBLE', 'ROLLBACK', 'ERROR', 'DELETEERROR', 'UNARCHIVED',
#             'NOTARCHIVABLE','LINKERROR')
reason = Enum('NOTINLDAP', 'NOMANDATORY', "FAILARCHIVE", "FAILDELETE", "FAILARCHIVEDN", "FAILDELETEDN", 'UNKNOWN',
              "ISARCHIVED", "UNKNOWNARCHIVED", "NODNINAD", "EXPLICITEXCLUDED", "INSERTBBDDSTORAGE", "NOTALLSERVICESOFF", "NOTARCHIVABLE")

CADUCADO = '3'
CANCELADO = '6'

# PARAMETROS DE LA EJECUCION
if TEST:
    sessionId = "PRUEBA1"
    fromDate = ""
    toDate = "2012-01-01"

    MOUNTS = ({'account': 'LINUX', 'fs': 'homenfs', 'label': 'HOMESNFSTEST', 'mandatory': True, 'val': ''},
              {'account': 'MAIL', 'fs': 'homemail', 'label': 'NEWMAILTEST', 'mandatory': False, 'val': ''})
else:
    MOUNTS = ({'account': 'LINUX', 'fs': 'homenfs', 'label': 'HOMESNFS', 'mandatory': True, 'val': ''},
              {'account': 'MAIL', 'fs': 'homemail', 'label': 'mail', 'mandatory': True, 'val': ''},
              {'account': 'WINDOWS', 'fs': 'perfiles', 'label': 'NEWPERFILES', 'mandatory': False, 'val': ''},
              {'account': 'WINDOWS', 'fs': 'perfilesv2', 'label': 'PERFILESV2', 'mandatory': False, 'val': ''},
              {'account': 'WINDOWS', 'fs': 'perfilesv5', 'label': 'PERFILESV5', 'mandatory': False, 'val': ''},
              {'account': 'WINDOWS', 'fs': 'homecifs', 'label': 'HOMESCIF', 'mandatory': True, 'val': ''})

    sessionId = ""
    fromDate = ""
    toDate = ""

#Globales de oracle
oracleCon = None
ldapCon = None
status=Status(["oracleCon",
                "ldapCon",
                "homenfs",
                "perfiles",
                "homecifs",
                "sessionId",
                "userList"],
                ["homemail"])

parentdir = {}
altdirs = {}
session = None
