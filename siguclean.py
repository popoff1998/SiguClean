#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
# from __future__ import print_function

# Defines (globales)
__version__="1.0.0"

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
LDAPRELAX = False

# SERVICIOS
OFFSERVICES = {'L01': 'N', 'L02': 'N', 'L03': 'N', 'L04': 'N', 'L05': 'N'}

# VARIABLES DE CONFIGURACION
Q_GET_BORRABLES = 'SELECT CCUENTA FROM UT_CUENTAS WHERE (CESTADO=\'4\' OR CESTADO=\'6\')'
Q_GET_CUENTA_NT = 'SELECT CCUENTA FROM UT_CUENTAS_NT WHERE CCUENTA = %s'
Q_INSERT_STORAGE = 'INSERT INTO UT_ST_STORAGE (IDSESION,CCUENTA,TTAR,NSIZE,CESTADO,NSIZE_ORIGINAL,NFICHEROS) VALUES %s'
Q_INSERT_SESION = 'INSERT INTO UT_ST_SESION (IDSESION,FSESION,FINICIAL,FFINAL,DSESION) VALUES %s'
Q_IGNORE_ARCHIVED = 'UF_ST_ULTIMA(CCUENTA) !=\'0\''
Q_ONLY_ARCHIVED = 'UF_ST_ULTIMA(CCUENTA) =\'0\''

# LDAP_SERVER = "ldap://ldap1.priv.uco.es"
LDAP_SERVER = "ldaps://ucoapp09.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
ORACLE_SERVER = 'ibmblade47/av10g'
ALTROOTPREFIX = '0_'
NTCHECK = 'ad'

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

# PARAMETROS DE LA EJECUCION
if TEST:
    sessionId = "PRUEBA1"
    fromDate = ""
    toDate = "2012-01-01"

    MOUNTS = ({'account': 'LINUX', 'fs': 'homenfs', 'label': 'HOMESNFSTEST', 'mandatory': True, 'val': ''},
              {'account': 'MAIL', 'fs': 'homemail', 'label': 'NEWMAILTEST', 'mandatory': False, 'val': ''})
else:
    MOUNTS = ({'account': 'LINUX', 'fs': 'homenfs', 'label': 'HOMESNFS', 'mandatory': True, 'val': ''},
              {'account': 'MAIL', 'fs': 'homemail', 'label': 'MAIL', 'mandatory': True, 'val': ''},
              {'account': 'WINDOWS', 'fs': 'perfiles', 'label': 'PERFILES', 'mandatory': False, 'val': ''},
              {'account': 'WINDOWS', 'fs': 'homecifs', 'label': 'HOMESCIF', 'mandatory': True, 'val': ''})

    sessionId = ""
    fromDate = ""
    toDate = ""

from shutil import rmtree
import tarfile
from pprint import pprint
import pickle
import re
import collections
import subprocess

from enum import Enum
import dateutil.parser
from progressbar import *

import config


state = Enum('NA', 'ARCHIVED', 'DELETED', 'TARFAIL', 'NOACCESIBLE', 'ROLLBACK', 'ERROR', 'DELETEERROR', 'UNARCHIVED',
             'NOTARCHIVABLE')

CADUCADO = '3'
CANCELADO = '6'


class Mrelax(Enum):
    NONE = '1'
    CANCELADOS = '2'
    TODOS = '3'


MANDATORYRELAX = Mrelax.NONE


class Cestado(Enum):
    CADUCADO = '4'
    CANCELADO = '6'


# FUNCIONES


def fetch_single(cursor):
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


def have_progress():
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


def _pprint(*_args):
    """Imprime de forma bonita algo, teniendo en cuenta si es iterable o no"""

    for arg in _args:
        if iterable(arg):
            pprint(arg),
        else:
            print(arg),


def check_environment():
    """Chequea el entorno de ejecucion (instancia unica)"""

    global CHECKED
    _print(1, "PASO1: Comprobando el entorno de ejecucion ...")
    if not CHECKED:
        check_modules()
        check_connections()
        check_mounts()
    CHECKED = True


def check_modules():
    """Comprueba que son importables los módulos ldap y cx_Oracle"""

    _print(1, "  Comprobando módulos necesarios")

    # python_ldap
    _print(1, '     comprobando modulo conexion a ldap  ... ', end=' ')
    try:
        global ldap
        import ldap

        _print(1, "CORRECTO")
    except ImportError:
        _print(0, "ABORT: No existe el modulo python-ldap, instalelo")
        ldap = None
        os._exit(False)

    # cx_Oracle
    _print(1, '     comprobando modulo conexion a Oracle ... ', end='')
    try:
        global cx_Oracle
        import cx_Oracle

        _print(1, "CORRECTO")
    except ImportError:
        _print('ABORT: No existe el modulo cx_Oracle, instalelo')
        cx_Oracle = None
        os._exit(False)


def open_ldap(reconnect):
    if reconnect:
        verb = "DEBUG-WARNING: Reabriendo"
    else:
        verb = "Abriendo"

    if DEBUG:
        debug(verb, " conexión a ldap")
    try:
        global ldapCon
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, 0)
        ldap.set_option(ldap.OPT_REFERRALS, 0)
        ldapCon = ldap.initialize(LDAP_SERVER)
        ldapCon.simple_bind_s(BIND_DN, WINDOWS_PASS)
        config.status.ldapCon = True
    except ldap.LDAPError, _e:
        _print(1, "ERROR: ", verb, " conexión a ldap")
        _print(_e)
        config.status.ldapCon = False


def check_connections():
    """Establece las conexiones a ldap y oracle"""

    _print(1, "  Comprobando conexiones")
    import cx_Oracle
    # LDAP
    global WINDOWS_PASS
    if not WINDOWS_PASS:
        WINDOWS_PASS = raw_input('     Introduzca la clave de windows (administrador): ')
    if WINDOWS_PASS != "dummy":
        _print(1, '     comprobando conexion a ldap ... ', end='')
        open_ldap(False)
        if config.status.ldapCon is True:
            _print(1, "CORRECTO")

    # Oracle
    global ORACLE_PASS
    if not ORACLE_PASS:
        ORACLE_PASS = raw_input('     Introduzca la clave de oracle (sigu): ')
    if ORACLE_PASS != "dummy":
        _print(1, '     comprobando conexion a oracle ... ', end='')
        try:
            global oracleCon
            oracleCon = cx_Oracle.connect('sigu/' + ORACLE_PASS + '@' + ORACLE_SERVER)
            config.status.oracleCon = True
            _print(1, "CORRECTO")
        except cx_Oracle.DatabaseError:
            _print(1, "ERROR")
            config.status.oracleCon = False


def get_mount_point(algo, exclude_regex):
    """Devuelve el punto de montaje que contiene algo en el export"""

    try:
        with open("/proc/mounts", "r") as ifp:
            for line in ifp:
                fields = line.rstrip('\n').split()
                if DEBUG:
                    debug("DEBUG-INFO: EXPORT: ", fields[0], " MOUNT: ", fields[1], " ALGO: ", algo)
                if algo in fields[0]:
                    # Es un posible montaje, vemos si esta excluido
                    ret = exclude_regex.search(fields[1])
                    if DEBUG:
                        debug("DEBUG-INFO: (get_mount_point): campo es ", fields[1], " ret es ", ret)
                    if ret is not None:
                        if DEBUG:
                            debug("DEBUG-INFO: EXCLUIDO")
                        pass
                    else:
                        if DEBUG:
                            debug("DEBUG-INFO: INCLUIDO")
                        return fields[1]
    except EnvironmentError:
        pass
    return None  # explicit


def check_mounts():
    """Comprueba que los puntos de montaje están accesibles"""

    _print(1, "  Comprobando el acceso a los Datos")
    try:
        regex = re.compile(MOUNT_EXCLUDE)
        if DEBUG:
            debug("DEBUG-INFO: Regex de exclusion es ", MOUNT_EXCLUDE, " y su valor es ", regex)
    except:
        _print(0, "ABORT: La expresion ", MOUNT_EXCLUDE, " no es una regex valida, abortamos ...")
        regex = None
        os._exit(False)
    salgo = False
    for mount in MOUNTS:
        _print(2, '     comprobando ' + mount['fs'] + ' ...', end='')
        mount['val'] = get_mount_point(mount['label'], regex)
        if mount['val'] is not None:
            _print(2, "Usando montaje ", mount['val'])
            exec ("config.status.%s = True" % (mount['fs']))
        else:
            exec ("config.status.%s = False" % (mount['fs']))
            _print(2, "NO ACCESIBLE")
            salgo = True
    if salgo:
        _print(0, 'ABORT: Algunos puntos de montaje no estan accesibles')
        os._exit(False)
    # Resumen de montajes
    if CONFIRM:
        verblevel = 0
    else:
        verblevel = 1
    _print(verblevel, "*** RESUMEN DE MONTAJES ***")
    for mount in MOUNTS:
        if len(mount['label']) < 8:
            tabs = "\t\t\t"
        else:
            tabs = "\t\t"
        _print(verblevel, mount['label'], tabs, mount['val'])
    if CONFIRM:
        confirm()


def input_parameter(param, text, mandatory):
    """Lee un parametro admitiendo que la tecla intro ponga el anterior"""

    while True:
        prevparam = param
        param = raw_input(text + '[' + param + ']: ')
        if param == '':
            param = prevparam
        if param == 'c':
            param = ''
        if param == '' and mandatory:
            continue
        else:
            return param


def enter_parameters():
    """Lee por teclado los parametros de ejecucion"""

    while True:
        global sessionId, fromDate, toDate
        print "PASO2: Parametros de la sesion ('c' para borrar)"
        sessionId = input_parameter(sessionId, "Identificador de sesion: ", True)
        fromDate = input_parameter(fromDate, "Fecha desde (yyyy-mm-dd): ", False)
        toDate = input_parameter(toDate, "Fecha hasta (yyyy-mm-dd): ", True)

        print '\nSessionId = [' + sessionId + ']'
        print 'fromDate = [' + fromDate + ']'
        print 'toDate = [' + toDate + ']'

        sal = raw_input('\nSon Correctos (S/n): ')
        if sal == 'S':
            return
        else:
            continue


def pager(_iterable, page_size):
    """Funcion paginador"""

    import itertools

    _args = [iter(_iterable)] * page_size
    fill_value = object()
    for group in itertools.izip_longest(fillvalue=fill_value, *_args):
        yield (elem for elem in group if elem is not fill_value)


def imprime(user_list):
    """Imprime usando un paginador"""

    my_pager = pager(user_list, 20)
    for page in my_pager:
        for i in page:
            _print(1, i)
        tecla = raw_input("----- Pulse intro para continuar (q para salir) ----------")
        if tecla == 'q':
            break


def size_to_human(size):
    """Convierte un numero de bytes a formato humano"""

    symbols = ('B', 'K', 'M', 'G', 'T')
    indice = 0
    # if EXTRADEBUG: debug("EXTRADEBUG-INFO: (size_to_human) Size antes de redondear es: ",size )
    while True:
        if size < 1024:
            string = str(round(size, 1)) + " " + symbols[indice]
            return string
        size /= 1024.0
        indice += 1


def human_to_size(size):
    """Convierte un tamaño en formato humano a bytes"""

    symbols = ('B', 'K', 'M', 'G', 'T')
    letter = size[-1:].strip().upper()
    num = size[:-1]
    try:
        assert num.isdigit() and letter in symbols
        num = float(num)
        prefix = {symbols[0]: 1}
        for i, s in enumerate(symbols[1:]):
            prefix[s] = 1 << (i + 1) * 10
        return int(num * prefix[letter])
    except:
        if DEBUG:
            debug('DEBUG-ERROR: (human_to_size) ', size, ' no es traducible')
        return False


def time_stamp():
    """Devuelve un timestamp"""

    return '[{0}]\t'.format(str(datetime.datetime.now()))


def _print(level, *_args, **kwargs):
    """Formatea y archiva los mensajes por pantalla"""

    global VERBOSE
    if not VERBOSE:
        VERBOSE = 0
    if kwargs != {}:
        trail = kwargs['end']
    else:
        trail = '\n'
    cadena = "".join(str(x) for x in _args)
    if VERBOSE >= level:
        print cadena + trail,

    if config.session:
        if hasattr(config.session, 'log'):
            config.session.log.write_log(cadena + trail, False)
            config.session.log.write_all_output(cadena + trail, False)


def debug(*_args, **kwargs):
    """Formatea y archiva los mensajes de debug"""

    global fDebug
    # Si tenemos verbose o no tenemos sesion sacamos la info por consola tambien
    if VERBOSE > 0 or not config.session:
        print "".join(str(x) for x in _args)
    # Si tenemos definido el log de la sesion lo grabamos en el fichero, en caso
    # contrario solo salen por pantalla
    # En sesiones restore no abrimos el fichero
    if config.session and not RESTORE:
        if not fDebug:
            fDebug = open(config.session.logsdir + "/debug", "w")

        if kwargs != {}:
            trail = kwargs['end']
        else:
            trail = '\n'
        fDebug.write(time_stamp())
        if fAllOutput is not None:
            fAllOutput.write(time_stamp())
        for string in _args:
            fDebug.write(str(string))
            if fAllOutput is not None:
                fAllOutput.write(str(string))
        fDebug.write(trail)
        if fAllOutput is not None:
            fAllOutput.write(trail)
        fDebug.flush()
        if fAllOutput is not None:
            fAllOutput.flush()


def dn_from_user(user):
    """Devuelve la DN de un usuario de active directory"""

    import ldap
    dn = tupla = result_type = None
    filtro = "(&(CN=" + user + ")(!(objectClass=contact)))"
    try:
        result_id = ldapCon.search(USER_BASE,
                                   ldap.SCOPE_SUBTREE,
                                   filtro,
                                   None)
        result_type, tupla = ldapCon.result(result_id, 1)
        dn, none = tupla[0]
        _status = True
    except:
        _status = False
    return _status, dn, tupla, result_type


def ldap_from_sigu(cuenta, attr):
    """Consulta un atributo ldap mediante sigu"""
    # La funcion uf_leeldap busca en people o people-deleted segun el valor
    # del tercer parametro (que sea B o no), asi que hacemos una busqueda en
    # el primero y despues en el segundo en caso de que no encontremos nada

    q_ldap_sigu = 'select uf_leeldap(' + comillas(cuenta) + ',' + comillas(attr) + ') from dual'

    cursor = oracleCon.cursor()
    cursor.execute(q_ldap_sigu)
    tmp_list = cursor.fetchall()
    tmp_list = tmp_list[0][0]
    if EXTRADEBUG:
        debug("DEBUG-INFO (ldapFromSigu1): ", cuenta, " tmp_list = ", tmp_list)
    if tmp_list is not None:
        cursor.close()
        return tmp_list.strip().split(':')[1].strip()
    # Hacemos la comprobacion en people-deleted
    q_ldap_sigu = 'select uf_leeldap(' + comillas(cuenta) + ',' + comillas(attr) + ',' + comillas('B') + ') from dual'
    cursor.execute(q_ldap_sigu)
    tmp_list = cursor.fetchall()
    tmp_list = tmp_list[0][0]
    if EXTRADEBUG:
        debug("DEBUG-INFO (ldapFromSigu2): ", cuenta, " tmp_list = ", tmp_list)
    cursor.close()
    return tmp_list.strip().split(':')[1].strip() if tmp_list else None


def scha_from_ldap(cuenta):
    """Devuelve un diccionario con los permisos de la cuenta"""

    q_ldap_sigu = 'select cServicio,tServicio,uf_valida_servicio_ldap(' + comillas(
        cuenta) + ',cServicio) from ut_servicios_mapa'
    # q_ldap_sigu = 'select sigu.ldap.uf_leeldap(\''+cuenta+'\',\'schacuserstatus\') from dual'
    cursor = oracleCon.cursor()
    cursor.execute(q_ldap_sigu)
    tmp_list = cursor.fetchall()
    # tmp_list = str(tmp_list[0][0]).replace(' schacuserstatus :','').split()
    # Convertimos en un diccionario
    x = "None"
    try:
        # ret = dict([tuple(x.split(":")) for x in tmp_list])
        ret = dict([(x[0], x[2]) for x in tmp_list])
        return ret
    except:
        # Vemos si no ha devuelto nada
        if x == "None":
            return None
        else:
            return False


def are_services_off(services):
    if len(services) < NUMSCHASERVICES:
        return len(services)
    for i in services:
        if services[i] != 'N' and i in OFFSERVICES:
            return False
    return True


def all_services_off(user):
    services = scha_from_ldap(user)
    # Si no devuelve nada, no tiene ningun servicio a off
    if services is None:
        return False
    else:
        return are_services_off(services)


def check_services(user_list):
    for user in user_list:
        try:
            services = scha_from_ldap(user)
            if services is None:
                _print(1, "INFO: Usuario ", user, " tiene todos los servicios en ON")
                continue
            if services is False:
                _print(1, "ERROR: Consultando los servicios de ", user)
                continue
            ret = are_services_off(services)
            if ret is False:
                _print(1, "INFO: Usuario ", user, " no tiene todos los servicios en OFF")
            elif ret is not True:
                _print(1, "INFO: Usuario ", user, " tiene menos servicios de los esperados a OFF")
        except BaseException, error:
            _print(0, "ERROR: Error desconocido consultando servicios del usuario ", user)
            _print(0, "ERRORCODE: ", error)


def get_list_by_date(to_date, from_date='1900-01-01'):
    """Devuelve una lista de cuentas entre dos fechas"""

    q_between_dates = 'FCADUCIDAD  BETWEEN to_date(\'' + from_date + \
                      '\',\'yyyy-mm-dd\') AND to_date(\'' + to_date + \
                      '\',\'yyyy-mm-dd\')'
    query = '{0} AND {1}'.format(Q_GET_BORRABLES, q_between_dates)
    if IGNOREARCHIVED is True:
        _print(1, 'INFO: Ignorando los ya archivados')
        query = '{0} AND {1}'.format(query, Q_IGNORE_ARCHIVED)
    if DEBUG:
        debug("DEBUG-INFO: (get_list_by_date) Query:", query)
    try:
        cursor = oracleCon.cursor()
        cursor.execute(query)
        tmp_list = cursor.fetchall()
        cursor.close()
    except BaseException, error:
        _print(0, "ERROR: Error recuperando la lista de usuarios")
        if DEBUG:
            debug("DEBUG-ERROR: (get_list_by_date): ", error)
        return None
    # Convertimos para quitar tuplas
    user_list = [x[0] for x in tmp_list]
    config.status.userList = True
    return user_list


def get_archived_by_date(to_date, from_date='1900-01-01'):
    """Devuelve una lista de cuentas entre dos fechas"""

    q_between_dates = 'FCADUCIDAD  BETWEEN to_date(\'' + from_date + \
                      '\',\'yyyy-mm-dd\') AND to_date(\'' + to_date + \
                      '\',\'yyyy-mm-dd\')'
    query = '{0} AND {1} AND {2}'.format(Q_GET_BORRABLES, q_between_dates, Q_ONLY_ARCHIVED)
    if DEBUG:
        debug("DEBUG-INFO: (get_list_by_date) Query:", query)
    try:
        cursor = oracleCon.cursor()
        cursor.execute(query)
        tmp_list = cursor.fetchall()
        cursor.close()
    except BaseException, error:
        _print(0, "ERROR: Error recuperando la lista de usuarios archivados")
        if DEBUG:
            debug("DEBUG-ERROR: (get_list_by_date): ", error)
        return None
    # Convertimos para quitar tuplas
    user_list = [x[0] for x in tmp_list]
    config.status.userList = True
    return user_list


def is_archived(user):
    """Comprueba si un usuario esta archivado. De momento solo manejo el código de salida
    de la función de sigu y no la información adicional. La casuistica es segun la salida
    de UF_ST_ULTIMA
    - 0 (ya archivado) o None: Devolvemos True (no procesar)
    - 1 o 2 (hay que archivar): Devolvemos False
    - 9 (el estado no es caducado o cancelado): Devolvemos la cadena de estado"""

    try:
        cursor = oracleCon.cursor()
        # Llamo a la función uf_st_ultima
        ret = cursor.callfunc('UF_ST_ULTIMA', cx_Oracle.STRING, [user])
        cursor.close()
        if ret == "0":
            return True
        else:
            if ret is None:
                # Devolvemos como archivado si el usuario no existe (para fromfile)
                return True
            if ret[0] == "9":
                # Si el estado es distinto de caducado o cancelado, devolvemos el propio estado
                return ret[1]
            # Si estamos aquí la salida es 1 o 2 y no esta archivado por tanto
            return False
    except BaseException, error:
        if DEBUG:
            debug("DEBUG-ERROR: (is_archived) ", error)
        return None


def is_expired(user):
    """Comprueba si un usuario esta expirado (caducado o cancelado)"""

    try:
        cursor = oracleCon.cursor()
        cursor.execute("select CESTADO from ut_cuentas where CCUENTA = " + comillas(user))
        ret = fetch_single(cursor)
    except BaseException, error:
        _print(0, "ERROR: Consultando estado archivable del usuario ", user)
        _print(0, "ERROR-CODE: ", error)
        return None

    cursor.close()
    if ret == "":
        return None
    if ret == '4' or ret == '6':
        return True
    else:
        return False


def has_archived_data(user):
    """Devuelve True si hay algún registro de archivado en ut_st_storage"""

    try:
        cursor = oracleCon.cursor()
        cursor.execute("select unique ccuenta from ut_st_storage where ccuenta = " + comillas(user))
        ret = fetch_single(cursor)
        if ret == "":
            return False
        else:
            return True
    except:
        return False


def has_cuenta_nt(cuenta):
    """Comprueba si un usuario tiene cuenta NT"""

    import ldap

    stat_ad = False
    stat_sigu = False

    if NTCHECK == 'sigu' or NTCHECK == 'both':
        query = Q_GET_CUENTA_NT % comillas(cuenta)
        cursor = oracleCon.cursor()
        cursor.execute(query)
        stat_sigu = cursor.fetchall()

    if NTCHECK == 'ad' or NTCHECK == 'both':
        filtro = "(&(CN=" + cuenta + ")(!(objectClass=contact)))"
        try:
            result_id = ldapCon.search(USER_BASE,
                                       ldap.SCOPE_SUBTREE,
                                       filtro,
                                       None)
            result_type, tupla = ldapCon.result(result_id, 1)
            if len(tupla) == 1:
                stat_ad = False
            else:
                stat_ad = True
        except:
            stat_ad = False

    if NTCHECK == 'sigu':
        return True if stat_sigu else False
    if NTCHECK == 'ad':
        return stat_ad
    return True if stat_ad and stat_sigu else False


def comillas(cadena):
    """ Nos devuelve una cadena entre comillas"""

    return '\'' + cadena + '\''


def values_list(*_args, **kwargs):
    """Devuelve una cadena con una serie de valores formateados para sentencia sql"""

    _ = kwargs
    cadena = '('
    size = len(_args)
    arg_number = 1
    for x in _args:
        # Parseamos primero para ver si puede ser una fecha
        try:
            _ = dateutil.parser.parse(x)
            # Es una fecha parseable
            cad = 'TO_DATE(' + comillas(x) + ',\'YYYY-MM-DD\')'
        except BaseException:
            if type(x) is str:
                cad = comillas(x)
            else:
                cad = str(x)
        cadena += cad
        if arg_number < size:
            cadena += ','
        arg_number += 1
    cadena += ')'
    return cadena


reason = Enum('NOTINLDAP', 'NOMANDATORY', "FAILARCHIVE", "FAILDELETE", "FAILARCHIVEDN", "FAILDELETEDN", 'UNKNOWN',
              "ISARCHIVED", "UNKNOWNARCHIVED", "NODNINAD", "EXPLICITEXCLUDED", "INSERTBBDDSTORAGE", "NOTALLSERVICESOFF")


def format_reason(user, _reason, attr, stats):
    """Formatea la razon de fallo devolviendo una cadena"""

    stats.reason[_reason._index] += 1
    return user + "\t" + _reason._key + "\t" + attr


def filter_archived(user_list):
    """Filtra de una lista de usuarios dejando solo los que no estan archivados
    descontando los que dan otro tipo de salida que no sea False"""

    user_list[:] = [x for x in user_list if is_archived(x) is False]


def from_file(user_list):
    """Lee la lista desde un fichero, teniendo en cuenta el filtro de exclusión"""

    if os.path.exists(FROMFILE):
        try:
            _f = open(FROMFILE, "r")
            # Leemos los usuarios quitando el \n final
            user_list.extend([line.strip() for line in _f])
            _f.close()
            # Si tenemos IGNOREARCHIVED filtramos la lista
            if IGNOREARCHIVED:
                filter_archived(user_list)
                if EXTRADEBUG:
                    debug("EXTRADEBUG-INFO: Lista filtrada: ", user_list)
            return True
        except BaseException, error:
            if DEBUG:
                debug("Error leyendo FROMFILE: ", error)
            _print(0, "Error leyendo FROMFILE: ", FROMFILE)
            return False
    else:
        _print(0, "El fichero FROMFILE ", FROMFILE, " no existe")
        return False


def unique_name(filename):
    """Devuelve un nombre unico para un fichero que se va a renombrar"""
    contador = 0
    while os.path.exists(filename + "." + str(contador)):
        contador += 1
    return filename + "." + str(contador)


# CLASES

class Stats(object):
    """Clase para llevar las estadísticas de una sesion"""

    def __init__(self, session):
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
        _print(0, "-------------------------------")
        _print(0, "ESTADISTICAS DE LA SESION")
        _print(0, "-------------------------------")
        _print(0, "Total:\t\t", self.total)
        _print(0, "Correctos:\t", self.correctos)
        _print(0, "Incorrectos:\t", self.total - self.correctos)

        _print(0, "\n--- Detalles de fallos ---\n")
        _print(0, "Failed:\t\t", self.failed)
        _print(0, "Rollback:\t", self.rollback)
        _print(0, "Norollback:\t", self.norollback)
        _print(0, "Excluded:\t", self.excluded)
        _print(0, "Skip:\t\t", self.skipped)
        _print(0, "Suma:\t\t", self.failed + self.rollback + self.norollback + self.skipped + self.excluded)

        _print(0, "\n--- Razones del fallo/exclusion ---\n")
        i = 0
        for r in self.reason:
            _print(0, reason[i], ":\t", r)
            i += 1

        _print(0, "\n--- Rendimiento ---\n")
        _print(0, "Inicio:\t\t", self.inicio.strftime('%d-%m-%y %H:%M:%S'))
        _print(0, "Fin\t\t", self.fin.strftime('%d-%m-%y %H:%M:%S'))
        elapsed = self.fin - self.inicio
        _print(0, "Elapsed:\t", elapsed)
        # El cálculo del rendimiento lo escalamos
        _users = self.total - self.skipped
        if _users > elapsed.seconds:
            _rendimiento = _users / elapsed.seconds
            _print(0, "Rendimiento:\t", _rendimiento, " users/sec")
        else:
            _rendimiento = (_users * 60) / elapsed.seconds
            if _rendimiento >= 1:
                _print(0, "Rendimiento:\t", _rendimiento, " users/min")
            else:
                _rendimiento = elapsed.seconds / _users
                _print(0, "Rendimiento:\t", _rendimiento, " sec/user")


class Log(object):
    """Clase que proporciona acceso a los logs"""

    def __init__(self, session):
        global fAllOutput
        self.session = session
        # Creamos el directorio logs si no existe. Si existe renombramos el anterior
        if CONSOLIDATE:
            session.logsdir = session.tardir + "/consolidatelogs"
        else:
            session.logsdir = session.tardir + "/logs"
        if not os.path.exists(session.logsdir):
            os.mkdir(session.logsdir, 0777)
        else:
            # Tenemos que tener en cuenta de si es una sesion restore
            # caso de no serla rotamos el log.
            # si  lo es, usamos el mismo log solo para cmdline ya que el fork lo rotara
            if not RESTORE:
                new_name = unique_name(session.logsdir)
                os.rename(session.logsdir, new_name)
                os.mkdir(session.logsdir, 0777)
        # Si es restore salimos sin crear fichero ninguno
        if RESTORE:
            return
        # Abrimos todos los ficheros
        # if not CONSOLIDATE:
        self.fUsersDone = open(session.logsdir + '/users.done', 'w')
        self.fUsersFailed = open(session.logsdir + '/users.failed', 'w')
        self.fUsersRollback = open(session.logsdir + '/users.rollback', 'w')
        self.fUsersNoRollback = open(session.logsdir + '/users.norollback', 'w')
        self.fUsersSkipped = open(session.logsdir + '/users.skipped', 'w')
        self.fUsersExcluded = open(session.logsdir + '/users.excluded', 'w')
        self.fUsersList = open(session.logsdir + '/users.list', 'w')
        self.fFailReason = open(session.logsdir + '/failreason', "w")

        self.fLogfile = open(session.logsdir + '/logfile', 'w')
        self.fBbddLog = open(session.logsdir + '/bbddlog', 'w')
        self.fAllOutput = open(session.logsdir + '/alloutput', "w")
        fAllOutput = self.fAllOutput
        self.fCreateDone = open(session.logsdir + '/create.done', "w")
        self.fRenameDone = open(session.logsdir + '/rename.done', "w")
        self.fRenameFailed = open(session.logsdir + '/rename.failed', "w")

    def write_create_done(self, string):
        self.fCreateDone.writelines(string + "\n")
        self.fCreateDone.flush()

    def write_rename_done(self, string):
        self.fRenameDone.writelines(string + "\n")
        self.fRenameDone.flush()

    def write_rename_failed(self, string):
        self.fRenameFailed.writelines(string + "\n")
        self.fRenameFailed.flush()

    def write_done(self, string):
        self.fUsersDone.writelines(string + "\n")
        self.fUsersDone.flush()
        self.session.stats.correctos += 1

    def write_failed(self, string):
        self.fUsersFailed.writelines(string + "\n")
        self.fUsersFailed.flush()
        self.session.stats.failed += 1

    def write_excluded(self, string):
        self.fUsersExcluded.writelines(string + "\n")
        self.fUsersExcluded.flush()
        self.session.stats.excluded += 1

    def write_fail_reason(self, string):
        self.fFailReason.writelines(string + "\n")
        self.fFailReason.flush()

    def write_rollback(self, string):
        self.fUsersRollback.writelines(string + "\n")
        self.fUsersRollback.flush()
        self.session.stats.rollback += 1

    def write_no_rollback(self, string):
        self.fUsersNoRollback.writelines(string + "\n")
        self.fUsersNoRollback.flush()
        self.session.stats.norollback += 1

    def write_skipped(self, string):
        self.fUsersSkipped.writelines(string + "\n")
        self.fUsersSkipped.flush()
        self.session.stats.skipped += 1

    def write_log(self, string, newline):
        # En sesiones restore no escribimos el log
        if not RESTORE:
            trail = "\n" if newline else ""
            self.fLogfile.write(string + trail)
            self.fLogfile.flush()

    def write_all_output(self, string, newline):
        # En sesiones restore no escribimos el log
        if not RESTORE:
            trail = "\n" if newline else ""
            self.fAllOutput.write(string + trail)
            self.fAllOutput.flush()

    def write_bbdd(self, string):
        try:
            self.fBbddLog.write(string + "\n")
            self.fBbddLog.flush()
        except IOError, error:
            _print(0, "ERROR: (write_bbdd)", "I/O Error({0}) : {1}".format(error.errno, error.strerror))
        except ValueError, error:
            _print(0, "ERROR: (write_bbdd)", "Error de valor: ", error)
        except AttributeError, error:
            _print(0, "ERROR: (write_bbdd)", "Error de atributo: ", error)

    @staticmethod
    def write_iterable(fhandle, _iterable):
        line = "\n".join(_iterable)
        line += "\n"
        fhandle.writelines(line)
        fhandle.flush()


class Session(object):
    """Clase para procesar una sesion de archivado"""

    def __init__(self, session_id, from_date, to_date):
        global MAXSIZE
        config.session = self
        self.sessionId = session_id
        self.fromDate = from_date
        self.toDate = to_date
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

        # Comprobamos los parametros para poder ejecutar
        if not self.sessionId:
            raise ValueError
        if not self.fromDate:
            self.fromDate = '1900-01-01'
        if not self.toDate:
            self.toDate = '2100-01-01'
        # Comprobamos que existe TARDIR
        if TARDIR is None:
            raise Exception("No ha dado valor a sessiondir")
        # Directorio para los tars
        if os.path.exists(TARDIR):
            if self.sessionId:
                self.tardir = TARDIR + '/' + self.sessionId
            if not os.path.isdir(self.tardir):
                os.mkdir(self.tardir, 0777)
        else:
            # Abortamos porque no existe el directorio padre de los tars
            _print(0, 'ABORT: (session-start) No existe el directorio para tars: ', TARDIR)
            os._exit(False)
        self.log = Log(self)
        self.stats = Stats(self)
        # Tratamos MAXSIZE
        # Intentamos convertir MAXSIZE a entero
        try:
            a = int(MAXSIZE)
            MAXSIZE = a
            if DEBUG is True:
                debug("MAXSIZE era un entero y vale ", MAXSIZE)
        except BaseException:
            # Es una cadena vemos si es auto, convertible de humano o devolvemos error
            if MAXSIZE == "auto":
                try:
                    statfs = os.statvfs(TARDIR)
                    MAXSIZE = int(statfs.f_bsize * statfs.f_bfree * 0.9)
                    if DEBUG:
                        debug("MAXSIZE era auto y vale ", MAXSIZE)
                except BaseException:
                    _print(0, "ABORT: Calculando MAXSIZE para ", TARDIR)
                    os._exit(False)
            else:
                a = human_to_size(MAXSIZE)
                if a is not False:
                    MAXSIZE = a
                    if DEBUG:
                        debug("MAXSIZE era sizehuman y vale ", MAXSIZE)
                else:
                    _print(0, "ABORT: opción MAXSIZE invalida: ", MAXSIZE)
                    os._exit(False)

        # Tratamos el fichero EXCLUDEUSERSFILE
        if EXCLUDEUSERSFILE is not None:
            _print(0, "Excluyendo usuarios de ", EXCLUDEUSERSFILE)
            if os.path.exists(EXCLUDEUSERSFILE):
                try:
                    _f = open(EXCLUDEUSERSFILE, "r")
                    # Leemos los usuarios quitando el \n final
                    self.excludeuserslist.extend([line.strip() for line in _f])
                    _f.close()
                except BaseException, error:
                    if DEBUG:
                        debug("Error leyendo EXCLUDEUSERSFILE: ", error)
                    _print(0, "Error leyendo EXCLUDEUSERSFILE: ", FROMFILE)
            else:
                _print(0, "ABORT: No existe el fichero de exclusion de usuarios")
                os._exit(False)

        _print(0, 'Procesando la sesion ', self.sessionId, ' desde ', self.fromDate, ' hasta ', self.toDate)

    def account_list_from_current(self):
        """Lee userlist en base a los usuarios archivados previamente"""

        # Comprobamos que no existe ya para sesiones que se hayan creado sin procesar.
        if not self.accountList:
            try:
                self.accountList = [s for s in os.listdir(self.tardir) if (
                    not s.startswith("logs") and not s.startswith("consolidatelogs") and os.path.isdir(
                        self.tardir + "/" + s))]
                return True
            except BaseException, error:
                _print(0, "ABORT: No puedo recuperar la lista de usuarios previamente archivados")
                _print(0, "ERROR: ", error)
                return False

    def get_session_id(self):
        """Devuelve el ID de la BBDD en base a la descripcion de sesion"""

        qgetidsesion = "SELECT IDSESION FROM UT_ST_SESION WHERE DSESION = %s"

        try:
            cursor = oracleCon.cursor()
            cursor.execute(qgetidsesion % comillas(self.sessionId))
            self.idsesion = fetch_single(cursor)
            cursor.close()
        except BaseException, error:
            _print(0, "ERROR: Recuperando id de sesion ", self.sessionId)
            _print(0, "ERROR: ", error)
            return False
        return self.idsesion

    def logdict(self, logs_dirs, pathname):
        from collections import defaultdict

        tmp_dict = defaultdict(list)
        lineas = 0
        for logsdir in logs_dirs:
            for line in open(self.tardir + "/" + logsdir + "/" + pathname).readlines():
                tmp_dict[line].append(None)
                lineas += 1
        return tmp_dict, lineas

    def consolidate_logs(self):
        """Consolida los logs de una sesión con múltiples logs
        (En desarrollo) """

        c_path = self.tardir + "/consolidatelogs"
        if os.path.exists(c_path):
            _print(0, "ABORT: Los logs ya están consolidados")
            return

        logs_dirs = [s for s in os.listdir(self.tardir) if s.startswith("logs")]

        if len(logs_dirs) == 1:
            _print(0, "ABORT: Solo hay una carpeta de logs, no es necesario consolidar")
            return

        usersdonedict, lineas = self.logdict(logs_dirs, 'users.done')
        print "Lineas: ", lineas," DoneDict: ",len(usersdonedict)

        usersfaileddict, lineas = self.logdict(logs_dirs, 'users.failed')
        print "Lineas: ", lineas, " FailedDict: ", len(usersfaileddict)

        userslistdict, lineas = self.logdict(logs_dirs, 'users.list')
        print "Lineas: ", lineas," ListDict: ",len(userslistdict)

        usersrollbackdict, lineas = self.logdict(logs_dirs, 'users.rollback')
        print "Lineas: ", lineas," RollbackDict: ",len(usersrollbackdict)

        ppp = set(usersrollbackdict).difference(set(usersdonedict))
        print "DifRollbackLen: ",len(ppp)
        return

    def consolidate_fs(self, fs):
        """Consolida un FS de una sesion previa"""

        import glob
        cursor = None

        _print(1, "\n**** CONSOLIDANDO ", fs, " ****\n")
        origin_dict = self.get_origin(fs)
        q_update = 'UPDATE UT_ST_STORAGE SET TTAR = %s WHERE TTAR = %s'
        oneshot = False
        # Hay que tener en cuenta que en el diccionario puede haber más entradas que en
        # los archivados, pues aquellos que fallaron posteriormente SI generaron
        # el log sobre el que nos hemos basado para averiguar los orígenes.
        # Por tanto recorreremos el FS y usaremos el dict para consultar el nuevo nombre

        archives = [s for s in os.listdir(self.tardir) if (not s.startswith("logs") and s != "consolidatelogs")]
        if DEBUG:
            debug("DEBUG: Archives len es: ", len(archives))

        # Vemos la diferencia entre uno y otro, estos serán los que no se han procesado.
        diff = set(origin_dict.keys()).difference(set(archives))
        if DEBUG:
            debug("DEBUG: La diferencia entre origenes y archives es: ", len(diff))
        # Abrimos el cursor
        try:
            cursor = oracleCon.cursor()
        except BaseException:
            _print(0, "ABORT: ConsolidateFs, Error abriendo cursor de oracle")
            os._exit(False)

        # Bucle de renombrado
        for archive in archives:
            path = self.tardir + "/" + archive + "/*_" + fs + "_*"
            try:
                origen = glob.glob(path)[0]
            except:
                _print(0, "WARNING: El usuario: ", archive, " no tiene ", fs)
                continue
            fich = origen
            fich = re.sub("_", "@", fich)
            # Si la entrada de diccionario no es None
            if origin_dict[archive]:
                destino = re.sub(fs, fs + "=" + origin_dict[archive], fich)
            else:
                destino = fich

            # Parámetros para el update de BBDD de sigu
            updateq = q_update % (comillas(destino), comillas(origen))

            # renombrar el fichero
            try:
                if not DRYRUN:
                    os.rename(origen, destino)
                    try:
                        self.log.write_bbdd(updateq + "\n")
                        cursor.execute(updateq)
                        self.log.write_rename_done(origen + ' ' + destino)
                    except BaseException, error:
                        _print(0, "ERROR: Haciendo update, error: ", error, " file: ", origen)
                        # deshago el renombrado
                        os.rename(destino, origen)
                        self.log.write_rename_failed(origen + ' ' + destino)
                    if DEBUG:
                        debug("DEBUG: Renombrando ", origen, " ---> ", destino)
                else:
                    # TEST: Para probar en pruebas el renombrado con dryrun
                    # os.rename(origen,destino)
                    self.log.write_bbdd(updateq + "\n")
                    _print(0, "INFO: Renombrando ", origen, " ---> ", destino)
                    _print(0, "INFO: UpdateQ= ", updateq)
                    self.log.write_rename_done(origen + ' ' + destino)
            except BaseException, error:
                _print(0, "ERROR: Renombrando ", origen, " a ", destino)
                _print(0, "ERROR: ", error)

            if not oneshot and ONESHOT:
                confirm()
                oneshot = True

        if not DRYRUN:
            cursor.close()
            oracleCon.commit()

        return True

    def consolidate(self, fslist):
        """Consolida una sesión. Le pasaremos una lista de los label de los fs a 
        procesar"""
        # Consolidamos los FS
        if fslist is not None:
            for fs in fslist:
                if not self.consolidate_fs(fs):
                    _print(0, "ABORT: Consolidando fs ", fs)
                    os._exit(False)

        # Consolidamos los logs
        if not self.consolidate_logs():
            _print(0, "ABORT: Consolidando los logs")
            os._exit(False)

        return True

    def check_services(self):
        archives = [s for s in os.listdir(self.tardir) if (not s.startswith("logs") and s != "consolidatelogs")]
        check_services(archives)

    def get_origin(self, fs):
        """Recupera la carpeta origen de un determinado archivado"""
        # Debo recorrer los ficheros logfile de todos los directorios logs
        # de la sesión, quedándome con las líneas que contienen "Archivando <storage>"
        # , extrayendo la cuarta columna y procesando esta para quitarle la primera parte que
        # es la raiz, despues tengo el alternativo (o el natural) y por ultimo el usuario
        global DEBUG
        from collections import defaultdict

        origindict = defaultdict(str)

        # Buscamos en mounts la base
        regex = re.compile(MOUNT_EXCLUDE)
        # Desactivamos temporalmente DEBUG
        tmp_debug = DEBUG
        DEBUG = False
        for mount in MOUNTS:
            if mount['fs'] == fs:
                raiz = get_mount_point(mount['label'], regex)
        # Recuperamos el valor de DEBUG
        DEBUG = tmp_debug

        logs_dirs = [s for s in os.listdir(self.tardir) if s.startswith("logs")]

        for logsdir in logs_dirs:
            for line in open(self.tardir + "/" + logsdir + "/logfile").readlines():
                if line.startswith("Archivando " + fs):
                    dummy, dummy, dummy, result, dummy = line.split(None, 4)
                    try:
                        dummy, origin, user = result.replace(raiz, '').split('/')
                    except ValueError:
                        # Si no puede desempaquetar es porque no se hizo de un alternativo
                        origin = None
                        dummy, user = result.replace(raiz, '').split('/')
                    origindict[user] = origin
        # En este punto ya tenemos un diccionario con las entradas únicas de
        # los origenes sacados de los logs
        return origindict

    def abort(self, severity):
        """Funcion que lleva el control sobre el proceso de abortar"""

        if EXTRADEBUG:
            debug("EXTRADEBUG-INFO: ABORTALWAYS ES: ", ABORTALWAYS)
        if ABORTLIMIT == 0:
            _print(0, 'ABORT: No abortamos porque ABORTLIMIT es 0')
            return

        if ABORTALWAYS is True:
            _print(0, 'ABORT: Error y ABORTALWAYS es True')
            os._exit(False)

        if ABORTINSEVERITY is True and severity is True:
            _print(0, 'ABORT: Error con severidad y ABORTINSEVERITY es True')
            os._exit(False)

        self.abortCount += 1
        if self.abortCount > self.abortLimit:
            _print(0, 'ABORT: Alcanzada la cuenta de errores para abort')
            os._exit(False)

    def die(self, user, rollback):
        """Funcion que controla si abortamos o no y gestiona los logs"""

        if rollback:
            if user.rollback():
                if DEBUG:
                    debug("DEBUG-INFO: Rollback exitoso de ", user.cuenta)
                self.log.write_rollback(user.cuenta)
                self.abort(False)
            else:
                if DEBUG:
                    debug("DEBUG-WARNING: Rollback fallido de ", user.cuenta)
                self.log.write_no_rollback(user.cuenta)
                self.abort(True)
        else:
            if user.exclude:
                self.log.write_excluded(user.cuenta)
            else:
                self.log.write_failed(user.cuenta)
        # Generamos y grabamos la razon del fallo
        if not user.failreason:
            self.log.write_fail_reason(format_reason(user.cuenta, reason.UNKNOWN, "----", self.stats))
        else:
            self.log.write_fail_reason(user.failreason)
        return False

    def get_account_list(self):
        if TEST:
            self.accountList = ['games', 'news', 'uucp', 'pepe']
            return True
        else:
            if FROMFILE is not None:
                # Leo la lista de usuarios de FROMFILE
                return from_file(self.accountList)
            else:
                # Recupero la lista de usuarios de SIGU
                self.accountList = get_list_by_date(self.toDate, self.fromDate)
                return True

    def bbdd_insert(self):
        """ Inserta un registro de archivado en la BBDD """
        now = datetime.datetime.now()
        # Distinguimos entre sesiones restoring y normales.
        # Para las normales generamos un nuevo indice.
        # Para las restoring usamos el previamente almacenado
        if DEBUG:
            debug("DEBUG-INFO: (session-bbdd_insert) RESTORING es: ", RESTORING)
        if not RESTORING and not CONSOLIDATE:
            try:
                cursor = oracleCon.cursor()
                # Consigo el idsesion
                self.idsesion = int(cursor.callfunc('UF_ST_SESION', cx_Oracle.NUMBER))
                values = values_list(self.idsesion, now.strftime('%Y-%m-%d'), self.fromDate,
                                     self.toDate, self.sessionId)
                query = Q_INSERT_SESION % values
                self.log.write_bbdd(query)
                if DRYRUN and not SOFTRUN:
                    return True
                cursor.execute(query)
                oracleCon.commit()
                cursor.close()
                # Salvamos en idsesion para restoring
                if DEBUG:
                    debug("DEBUG-INFO: (session-bbdd_insert) salvando idsesion: ", self.idsesion)
            except BaseException, error:
                _print(0, "ERROR: Almacenando en la BBDD sesion ", self.sessionId)
                if DEBUG:
                    debug("DEBUG-ERROR: (sesion.bbdd_insert) Error: ", error)
                return False
        else:
            # Leemos el valor de la sesion en curso
            self.idsesion = int(self.get_session_id())
            if DEBUG:
                debug("DEBUG-INFO: Leido ID sesion: ", self.idsesion)

        _f = open(self.logsdir + "/idsesion", "w")
        _f.write(str(self.idsesion) + "\n")
        _f.close()
        return True

    def start(self):
        # Directorio para TARS
        pbar = None
        if DEBUG:
            debug('DEBUG-INFO: (session.start) TARDIR es: ', TARDIR)
        print "VERBOSE: ", VERBOSE, "DEBUG: ", DEBUG, "PROGRESS: ", PROGRESS
        if have_progress():
            pbar = ProgressBar(widgets=[Percentage(), " ", Bar(marker=RotatingMarker()), " ", ETA()],
                               maxval=1000000).start()
        if not CONSOLIDATE:
            # Creo la lista de cuentas
            if not self.accountList:
                ret = self.get_account_list()
            else:
                ret = False
            # Si ret es False ha fallado la recuperacion de la lista de cuentas
            if not ret:
                _print(0, "ABORT: No he podido recuperar la lista de usuarios. Abortamos ...")
                os._exit(False)
        # Si la lista esta vacia no hay nada que procesar y salimos inmediatamente
        if len(self.accountList) == 0:
            _print(0, "EXIT: La lista de usuarios a procesar es vacia")
            os._exit(True)
        # Comenzamos el procesamiento
        self.log.write_iterable(self.log.fUsersList, self.accountList)
        self.stats.total = len(self.accountList)
        if have_progress():
            pbar.update(100000)
        # Creo la lista de objetos usuario a partir de la lista de cuentas
        pp = 100000
        ip = 100000 / len(self.accountList)
        if not self.userList:
            for account in self.accountList:
                if have_progress():
                    pp += ip
                    pbar.update(pp)
                user = User(account, self)
                # Manejamos la exclusion del usuario
                if user.exclude:
                    self.log.write_excluded(user.cuenta)
                    # Generamos y grabamos la razon del fallo
                    self.log.write_fail_reason(format_reason(user.cuenta, user.failreason, "----", self.stats))
                else:
                    self.userList.append(user)
        if have_progress():
            pbar.update(200000)
        # Insertamos sesion en BBDD
        self.bbdd_insert()
        # Proceso las entradas
        skip = False
        pp = 200000
        ip = 800000 / len(self.userList)

        # Bucle principal de procesamiento de usuarios
        for user in self.userList:
            # Salimos si hemos creado el fichero indicador de parada
            if os.path.exists(self.tardir + "/STOP"):
                os.remove(self.tardir + "/STOP")
                _print(0, "Abortado por el usuario con fichero STOP")
                os._exit(True)
            # Escribimos en user.current el usuario actual por si el programa
            # casca en medio del procesamiento y el usuario se queda a medio hacer
            _f = open(self.logsdir + "/users.current", "w")
            _f.write(user.cuenta)
            _f.close()

            if have_progress():
                pp += ip
                pbar.update(pp)
            if skip:
                self.log.write_skipped(user.cuenta)
                continue
            # Chequeamos ...
            _print(1, "*** PROCESANDO USUARIO ", user.cuenta, " ***")
            if not user.check():
                if not self.die(user, False):
                    continue
            # BORRAR: COMPROBACION DE STORAGES
            if not PROGRESS:
                print "*** STORAGES DE ", user.cuenta
                for st in user.storage:
                    st.display()
                print "************************"
            # ... Archivamos ...
            if not user.archive(self.tardir):
                _print(0, "ERROR: Archivando usuario ", user.cuenta)
                if not self.die(user, True):
                    continue
            self.tarsizes = self.tarsizes + user.tarsizes
            # ... Borramos storage ...
            if not user.delete_storage():
                _print(0, "ERROR: Borrando storages de usuario ", user.cuenta)
                if not self.die(user, True):
                    continue
            # Lo siguiente solo lo hacemos si tiene cuenta windows
            if 'WINDOWS' in user.cuentas and not CONSOLIDATE:
                # Si falla el archivado de DN continuamos pues quiere decir que no está en AD
                # Si ha hecho el archivado y falla el borrado, hacemos rollback
                if not user.archive_dn(self.tardir):
                    if DEBUG:
                        debug("DEBUG-WARNING: Error archivando DN de ", user.cuenta)
                    if not self.die(user, False):
                        pass
                        # continue
                else:
                    if not user.delete_dn():
                        if DEBUG:
                            debug("DEBUG-WARNING: Error borrando DN de ", user.cuenta)
                        if not self.die(user, True):
                            user.borra_cuenta_windows()
                            continue
            # Escribimos el registro de usuario archivado
            if not user.bbdd_insert():
                _print(0, "ERROR: Insertando storages en BBDD de usuario ", user.cuenta)
                if not self.die(user, True):
                    continue
            # Si hemos llegado aquí esta correcto
            if ABORTDECREASE:
                self.abortCount -= 1
            if self.abortCount < 0:
                self.abortCount = 0
            if DEBUG:
                debug('DEBUG-INFO: (session.start) abortCount: ' + str(self.abortCount))
            self.log.write_done(user.cuenta)
            # Controlamos si hemos llegado al tamaño maximo
            if MAXSIZE > 0:
                if self.tarsizes > MAXSIZE:
                    skip = True
        _print(1, 'Tamaño de tars de la session ', self.sessionId, ' es ', size_to_human(self.tarsizes))
        self.stats.fin = datetime.datetime.now()
        self.stats.show()


class Storage(object):
    def __init__(self, key, path, link, mandatory, parent):
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
            # Superseed de MANDATORYRELAX
        if MANDATORYRELAX == Mrelax.TODOS or (MANDATORYRELAX == Mrelax.CANCELADOS and self.parent.cEstado == CANCELADO):
            self.mandatory = False

    def display(self):
        _print(1, self.key, "\t = ", self.path, "\t Accesible: ", self.accesible, "\t Estado: ", self.state)

    def archive(self, rootpath):
        """ Archiva un storage en un tar"""
        # Vuelvo a comprobar aqui que es accesible
        if not self.accesible:
            self.state = state.NOACCESIBLE
            return False
        self.tarpath = rootpath + '/' + self.parent.cuenta + '@' + self.key + '@' + sessionId + ".tar.bz2"
        _print(1, "Archivando ", self.key, " from ", self.path, " in ", self.tarpath, " ... ")
        try:
            if DRYRUN and not SOFTRUN:
                # Calculo el tamaño sin comprimir y creo un fichero vacio para la simulacion
                _f = open(self.tarpath, "w")
                config.session.log.write_create_done(self.tarpath)
                _f.close()
                self.tarsize = os.lstat(self.tarpath).st_size
                self.state = state.ARCHIVED
                # self.bbdd_insert()
                return True
            # Cambiamos temporalmente al directorio origen para que el tar sea relativo
            with cd_change(self.path):
                tar = tarfile.open(self.tarpath, "w:bz2")

                # Gestionamos la exclusión de los que comienzan por .nfs para evitar errores de I/O
                tar.add(".", exclude=lambda x: os.path.basename(x).startswith(".nfs"))

                # Calculamos el tamaño original y el numero de ficheros
                members = tar.getmembers()
                for member in members:
                    self.size_orig = self.size_orig + member.size
                self.files = len(members)
                # Fin del calculo
                tar.close()
                config.session.log.write_create_done(self.tarpath)
            self.tarsize = os.path.getsize(self.tarpath)
            self.state = state.ARCHIVED
            # Muevo el almacenamiento en la BBDD al final del proceso del usuario para que sea mas transaccional
            # self.bbdd_insert()
            return True
        except BaseException, error:
            _print(0, "ERROR: Archivando ", self.key)
            if DEBUG:
                debug("DEBUG-ERROR: ", error)
            self.state = state.TARFAIL
            return False

    def bbdd_insert(self, cursor):
        """ Inserta un registro de archivado en la BBDD """
        # Solo procesamos si el storage se completo y por tanto esta en estado deleted
        if EXTRADEBUG:
            debug("EXTRADEBUG-INFO: (user-bbdd_insert) self.state: ", self.state, " key: ", self.key)
        if self.state == state.DELETED:
            try:
                # Como en sigu
                values = values_list(config.session.idsesion, self.parent.cuenta, self.tarpath, self.tarsize,
                                     self.state._index, self.size_orig, self.files)
                query = Q_INSERT_STORAGE % values
                config.session.log.write_bbdd(query)
                if DRYRUN and not SOFTRUN:
                    return True
                cursor.execute(query)
                # Hago el commit en el nivel superior
                # oracleCon.commit()
                # cursor.close()
                return True
            except BaseException, error:
                _print(0, "ERROR: Almacenando en la BBDD storage ", self.key)
                if DEBUG:
                    debug("DEBUG-ERROR: (storage.bbdd_insert) Error: ", error)
                return False
        else:
            # Si no estaba archivado y no cascó lo consideramos correcto
            return True

    def delete(self):
        """Borra un storage"""
        # Primero tengo que controlar si no existe y no es mandatory
        if DEBUG:
            debug("DEBUG-INFO: (storage.delete) ", self.key, " en ", self.path)
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
        except BaseException, error:
            if DEBUG:
                debug("DEBUG_ERROR: Borrando ", self.path, " : ", error)
            self.state = state.DELETEERROR
            return False

    def rollback(self):
        """Deshace el archivado borra los tars.
        - Si se ha borrado hacemos un untar
        - Borramos el tar
        - Ponemos el state como rollback"""
        if EXTRADEBUG:
            debug('EXTRADEBUG-INFO: (storage.rollback)', self.__dict__)
        if self.state in (state.DELETED, state.DELETEERROR):
            if not self.unarchive():
                self.state = state.ERROR
                return False
            # Restauro el link si existe
            if self.link is not None:
                if not DRYRUN:
                    os.link(self.link, self.path)
        try:
            # Si no está archivado no hay que borrar el tar
            if self.state not in (state.ARCHIVED, state.TARFAIL, state.UNARCHIVED):
                if DEBUG:
                    debug("DEBUG-INFO: (storage.rollback) No borro ", self.key,
                          " no estaba archivado, state = ", self.state)
                return True
            # Borramos el tar
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
        if self.state in (state.DELETED, state.ARCHIVED, state.DELETEERROR):
            try:
                _print(1, "Unarchiving ", self.key, " to ", self.path, " from ", self.tarpath, " ... ")
                if DRYRUN:
                    return True
                tar = tarfile.open(self.tarpath, "r:*")
                tar.extractall(self.path)
                tar.close()
                self.state = state.UNARCHIVED
                return True
            except:
                _print(0, "Error unarchiving ", self.key, " to ", self.path, " from ", self.tarpath, " ... ")
                return False

    def oldexist(self):
        """Comprueba la accesibilidad de un storage
        se tiene en cuenta que si no existe en el sitio por defecto
        puede existir en los root alternativos"""
        # Existe el path
        if DEBUG:
            debug("DEBUG-INFO: ****** PROCESANDO ", self.path, " *******")
        if os.path.exists(self.path):
            self.accesible = True
            return True
        # Aun no existiendo puede estar en un directorio movido, lo buscamos
        parent_dir = os.path.dirname(self.path)
        basename = os.path.basename(self.path)
        # Nos aseguramos de que si ya hemos buscado y no hay alternativos salir
        if parent_dir in config.altdirs and not config.altdirs[parent_dir]:
            if DEBUG:
                debug("DEBUG-WARNING: User:", self.parent.cuenta, " No existe path directo ni alternativo para ",
                      self.key, " en ", parent_dir)
            self.accesible = False
            return False
        if DEBUG:
            debug("DEBUG-INFO: User:", self.parent.cuenta, " No existe path directo para ", self.key,
                  " en ", self.path, " busco alternativo ...")
        # Buscamos en directorios alternativos del parent_dir
        # esta busqueda puede ser gravosa si se debe repetir para cada usuario por
        # lo que una vez averiguados los alternativos se deben de almacenar globalmente
        if parent_dir not in config.altdirs:
            if DEBUG:
                debug("DEBUG-INFO: No he construido aun la lista alternativa para ", self.key,
                      " en ", parent_dir, " lo hago ahora ...")
            config.altdirs[parent_dir] = [s for s in os.listdir(parent_dir)
                                          if s.startswith(ALTROOTPREFIX)]
        # Si la lista esta vacia salimos directamente
        if not config.altdirs[parent_dir]:
            if DEBUG:
                debug("DEBUG-WARNING: No existen directorios alternativos para ", self.key, " en ", parent_dir)
            self.accesible = False
            return False
        # Buscamos si existe en cualquiera de los directorios alternativos
        for path in config.altdirs[parent_dir]:
            joinpath = os.path.join(parent_dir, path, basename)
            if os.path.exists(joinpath):
                if DEBUG:
                    debug("DEBUG-INFO: User:", self.parent.cuenta, " encontrado alternativo para ",
                          self.key, " en ", joinpath)
                self.path = joinpath
                self.accesible = True
                return True
        # Si llegamos aqui es que no existe
        self.accesible = False
        return False

    def accesible_now(self):
        """Comprueba si esta accesible en este momento"""
        if os.path.exists(self.path):
            self.accesible = True
            return True
        else:
            self.accesible = False
            return False

    def exist(self):
        """Construye el storage"""
        # Buscamos todos los storages para ese FS
        parent_dir = os.path.dirname(self.path)
        basename = os.path.basename(self.path)
        direct_path = os.path.exists(self.path)
        first_path = True

        # Si estamos en una sesion de consolidacion es posible que el usuario
        # haya "revivido" desde que se archivo. Por tanto si se trata de la ubicacion
        # normal debemos saltarla.

        if direct_path and CONSOLIDATE:
            if DEBUG:
                debug("DEBUG-INFO: INCONSISTENCIA, el usuario ", self.parent.cuenta,
                      " ha resucitado, no proceso ", self.key)
            direct_path = False

        # Como no hay path directo ni ubicaciones alternativas para este tipo de storage
        # salimos  de la funcion retornando false.

        if not direct_path and parent_dir in config.altdirs and not config.altdirs[parent_dir]:
            if DEBUG:
                debug("DEBUG-WARNING: User:", self.parent.cuenta, " No existe path directo ni alternativo para ",
                      self.originalkey, " en ", parent_dir)
            self.accesible = False
            return False

        # Buscamos en directorios alternativos del parent_dir
        # esta busqueda puede ser gravosa si se debe repetir para cada usuario por
        # lo que una vez averiguados los alternativos se deben de almacenar globalmente

        if parent_dir not in config.altdirs:
            if DEBUG:
                debug("DEBUG-INFO: No he construido aun la lista alternativa para ", self.originalkey, " en ",
                      parent_dir, " lo hago ahora ...")
            config.altdirs[parent_dir] = [s for s in os.listdir(parent_dir)
                                          if s.startswith(ALTROOTPREFIX)]

            # Comprobamos el directo
        if direct_path:
            if DEBUG:
                debug("DEBUG-INFO: Encontrado path directo para ", self.originalkey)
            self.accesible = True
            self.directstorage = True
            first_path = False

        # Existen ubicaciones alternativas?
        if config.altdirs[parent_dir]:
            if DEBUG:
                debug("DEBUG-INFO: Existen ubicaciones alternativas de ", self.originalkey, " cuenta: ",
                      self.parent.cuenta)

            # Buscamos si existe en cualquiera de los directorios alternativos
            for path in config.altdirs[parent_dir]:
                joinpath = os.path.join(parent_dir, path, basename)
                if os.path.exists(joinpath):
                    if DEBUG:
                        debug("DEBUG-INFO: User:", self.parent.cuenta, " encontrado alternativo para ",
                              self.originalkey, " en ", joinpath)
                    # Tenemos que discriminar si es el primero o no
                    if first_path:
                        # El storage alternativo encontrado es el primero de la lista
                        # damos el cambiazo del directo, que no se va a procesar, por los
                        # atributos del alternativo
                        self.path = joinpath
                        self.key = self.originalkey + "=" + path
                        self.accesible = True
                        first_path = False
                    else:
                        # Este alternativo no es el primero por lo que va a la lista
                        # morestoragelist
                        altstorage = Storage(self.originalkey + "=" + path, joinpath, None, self.mandatory, self.parent)
                        altstorage.accesible = True
                        altstorage.secondary = True
                        self.morestoragelist.append(altstorage)
        else:
            if DEBUG:
                debug("DEBUG-INFO: No existen ubicaciones alternativas de ", self.originalkey, " cuenta: ",
                      self.parent.cuenta)

        if not self.morestoragelist:
            if DEBUG:
                debug("DEBUG-INFO: No existen storages adicionales para ", self.parent.cuenta, " de ",
                      self.originalkey, " en ", parent_dir)

        if not self.accesible:
            return False
        else:
            return True


class User(object):
    # Lo quito a ver que pasa global status
    instancias = {}

    def __new__(cls, name, parent):
        if name in User.instancias:
            return User.instancias[name]
        self = object.__new__(cls)
        User.instancias[name] = self
        return self

    def check(self):
        """Metodo que chequea los storages mandatory del usuario
        y si el usuario fue previamente archivado o no
        Asumo que la DN está bien porque acabo de buscarla."""

        # es archivable? Si no tiene todos los servicios a off, aun caducado o cancelado no debemos procesarlo
        if not PROGRESS:
            print "ESTADO USUARIO: ", self.cEstado
            print "MRELAX: ", MANDATORYRELAX, "TIPO: ", type(MANDATORYRELAX)
            print "CANCELADO ES: ", CANCELADO
            print "mRelax.TODOS: ", Mrelax.TODOS

        if not all_services_off(self.cuenta):
            self.exclude = True
            if DEBUG:
                debug("DEBUG-WARNING: (user.check) El usuario ", self.cuenta,
                      " no tiene todos los servicios a off")
            self.failreason = format_reason(self.cuenta, reason.NOTALLSERVICESOFF, "---", self.parent.stats)

            # Este usuario se va a excluir, pero en una sesion de consolidacion al ser esta comprobacion nueva
            # hay que tener en cuenta que ya se archivo algo. Se me dice que aunque asi fuera, que solo se marque la
            # razon de la exclusion y se deje lo demas como esta.   A mi no me convence porque deja flecos, pero bueno.

            return False

        # Esta archivado?
        archived = is_archived(self.cuenta)
        # Si la sesion es de consolidacion pasamos del chequeo de si esta archivado o no
        if not CONSOLIDATE:
            if archived is True:
                status = False
                self.failreason = format_reason(self.cuenta, reason.ISARCHIVED, "---", self.parent.stats)
                if DEBUG:
                    debug("DEBUG-WARNING: (user.check) El usuario ", self.cuenta, " ya estaba archivado")
                self.exclude = True
                return status
            elif archived is None:
                status = False
                self.failreason = format_reason(self.cuenta, reason.UNKNOWNARCHIVED, "---", self.parent.stats)
                if DEBUG:
                    debug("DEBUG-ERROR: (user.check) Error al comprobar estado de archivado de ", self.cuenta)
                self.exclude = True
                return status
            elif archived is not False:
                status = False
                self.failreason = format_reason(self.cuenta, reason.NOTARCHIVABLE, "---", archived)
                if DEBUG:
                    debug("DEBUG-WARNING: (user.check) El usuario ", self.cuenta,
                          " no es archivable, estado de usuario: ", archived)
                self.exclude = True
                return status

        # El usuario no esta archivado, compruebo sus storages
        status = True
        for storage in self.storage:
            # Si es secundario ya esta procesado y pasamos de el.
            if storage.secondary:
                continue
            # Aquí viene la nueva comprobación en función de como tengamos el mandatoryrelax
            # Si existe o no es mandatory, continuamos

            if not storage.exist() and storage.mandatory:
                # Si estamos aquí la cuenta es caducada o no hemos relajado el mandatory,
                # aparte de que el storage no existe y es mandatory, por tanto fallamos
                status = False
                self.failreason = format_reason(self.cuenta, reason.NOMANDATORY, storage.key, self.parent.stats)
                if DEBUG:
                    debug("DEBUG-WARNING: (user.check) El usuario ", self.cuenta, " no ha pasado el chequeo")
                break
            else:
                # Con el nuevo modelo, storage puede tener una lista de mas storages
                # así que lo proceso
                if storage.morestoragelist:
                    for st in storage.morestoragelist:
                        self.storage.append(st)

        return status

    def bbdd_insert(self):
        """Archiva en la BBDD todos los storages de usuario archivados"""
        cursor = oracleCon.cursor()
        for storage in self.storage:
            ret = storage.bbdd_insert(cursor)
            if not ret:
                # Debo hacer un rollback
                if DEBUG:
                    debug("DEBUG-ERROR: (user-bbdd_insert) Insertando: ", storage.key)
                oracleCon.rollback()
                cursor.close()
                self.failreason = format_reason(self.cuenta, reason.INSERTBBDDSTORAGE, storage.key, self.parent.stats)
                return False
        # Debo hacer un commit
        oracleCon.commit()
        cursor.close()
        return True

    def borra_cuenta_windows(self):
        # TODO
        """Borra la cuenta windows de la BBDD sigu
        Pendiente de implementar"""
        _ = self
        return True

    def list_cuentas(self):
        """Devuelve una tupla con las cuentas que tiene el usuario
        Por defecto tenemos correo y linux, para ver si tenemos windows
        consultamos si existe en la tabla UT_CUENTAS_NT, en AD o en ambos"""

        if TEST:
            return 'LINUX', 'MAIL'  # dummy return

        if has_cuenta_nt(self.cuenta):
            return "LINUX", "MAIL", "WINDOWS"
        else:
            return "LINUX", "MAIL"

    def estado(self):
        q_cestado = "select cestado from ut_cuentas where ccuenta = %s" % comillas(self.cuenta)
        cursor = oracleCon.cursor()
        cursor.execute(q_cestado)
        return fetch_single(cursor)

    def __init__(self, cuenta, parent):
        try:
            _ = self.cuenta
            if DEBUG:
                debug("DEBUG-WARNING: (user.__init__) YA EXISTIA USUARIO ", self.cuenta, " VUELVO DE INIT")
            return
        except:
            pass
        self.parent = parent
        self.exclude = False
        self.dn = None
        self.adObject = None
        self.failreason = None
        self.cuenta = cuenta
        self.tarsizes = 0

        # OJO: Aquí relleno el estado consultándolo de sigu usuario a usuario
        # sería más óptimo hacerlo en getlistbydate, pero de momento para no
        # modificar mucho lo hago así.

        self.cEstado = self.estado()

        # Compruebo si esta explicitamente excluido

        if self.cuenta in self.parent.excludeuserslist:
            self.failreason = reason.EXPLICITEXCLUDED
            self.exclude = True
            return

        if LDAPRELAX:
            self.homedir = cuenta
        else:
            try:
                self.homedir = os.path.basename(ldap_from_sigu(cuenta, 'homedirectory'))
            except BaseException:
                self.failreason = reason.NOTINLDAP
                self.exclude = True
                return
        self.storage = []
        self.rootpath = ''
        self.cuentas = self.list_cuentas()
        pase_por_aqui = False
        for c in self.cuentas:
            # relleno el diccionario storage
            for m in MOUNTS:
                sto_link = None
                # Si el montaje no esta paso a la siguiente cuenta
                if m['val'] is None:
                    continue
                if c == m['account']:
                    sto_path = m['val'] + '/' + self.homedir
                    sto_key = m['fs']
                    # Si es un enlace lo sustituyo por el path real
                    if os.path.islink(sto_path):
                        sto_link = sto_path
                        sto_path = os.path.realpath(sto_path)
                    # Caso especial de WINDOWS (calcular dn)
                    if c == 'WINDOWS':
                        # En el caso de windows el homedir es siempre la cuenta
                        sto_path = m['val'] + '/' + self.cuenta
                        status, dn, tupla, result_type = dn_from_user(self.cuenta)
                        if status:
                            self.dn = dn
                        else:
                            _print(0, "El usuario ", self.cuenta, "no tiene DN en AD y debería tenerla")
                            self.dn = False
                        if DEBUG and not pase_por_aqui:
                            debug("DEBUG-INFO: Usuario: ", self.cuenta, " DN: ", self.dn)
                            pase_por_aqui = True
                    storage = Storage(sto_key, sto_path, sto_link, m['mandatory'], self)
                    self.storage.append(storage)

        # Rellenamos el dn
        if self.dn:
            status, self.dn, self.adObject, result_type = dn_from_user(self.cuenta)

    def delete_storage(self):
        """Borra todos los storages del usuario"""

        for storage in self.storage:
            if storage.delete():
                continue
            else:
                if DEBUG:
                    debug("DEBUG-ERROR: (user.delete_storage) user:", self.cuenta, " Storage: ", storage.key)
                self.failreason = format_reason(self.cuenta, reason.FAILDELETE, storage.key, self.parent.stats)
                return False
        return True

    def show_storage(self):
        for storage in self.storage:
            storage.display()

    def show(self):
        _print(1, 'Cuenta\t=\t', self.cuenta)
        _print(1, 'DN\t=\t', self.dn)
        self.show_storage()

    def rollback(self):
        """Metodo para hacer rollback de lo archivado
        El rollback consiste en:
            - Recuperar de los tars los storages borrados            
            - Borrar los tars"""

        _print(2, "*** ROLLBACK INIT *** ", self.cuenta)
        for storage in self.storage:
            if not storage.rollback():
                return False
        # Si llegamos aquí, borramos el directorio padre
        try:
            rmtree(self.rootpath)
        except BaseException, error:
            _print(0, "ABORT: Error borrando rootpath: ", self.rootpath, " error: ", error)
            os._exit(False)
        _print(2, "*** ROLLBACK OK *** ", self.cuenta)
        return True

    def get_root_path(self, tardir):
        if not os.path.isdir(tardir):
            _print(0, "ABORT: (user-archive) No existe el directorio para TARS", tardir)
            os._exit(False)

        self.rootpath = tardir + '/' + self.cuenta
        if not os.path.isdir(self.rootpath):
            os.mkdir(self.rootpath, 0777)

    def unarchive(self, tardir):
        """Este metodo es useless pues debe rellenar los storages primero"""
        # Vemos si rootpath existe
        if not self.rootpath:
            self.get_root_path(tardir)

        for storage in self.storage:
            storage.unarchive()

    def archive(self, tardir):
        """Metodo que archiva todos los storages del usuario"""

        self.tarsizes = 0
        storage = None
        ret = False
        # Vemos si rootpath existe
        if not self.rootpath:
            self.get_root_path(tardir)

        for storage in self.storage:
            if not storage.archive(self.rootpath) and storage.mandatory:
                if DEBUG:
                    debug("DEBUG-INFO: (user.archive) mandatory de ", storage.key, " es ", storage.mandatory)
                self.failreason = format_reason(self.cuenta, reason.FAILARCHIVE, storage.key, self.parent.stats)
                ret = False
                break
            else:
                ret = True
                self.tarsizes = self.tarsizes + storage.tarsize

        if not ret:
            _print(0, 'WARNING: Error archivando usuario ', self.cuenta, ' fs ', storage.key, ' haciendo rollback')
            # Originalmente hacía aquí un rollback directamente y devolvía True.
            # Devuelvo False y gestiono el rollback en la función die
            # self.rollback()
            return False
        else:
            _print(2, 'INFO: El tamaño de los tars para ', self.cuenta, ' es: ', self.tarsizes)
            return True

    def archive_dn(self, tardir):
        """Usando pickle archiva el objeto DN de AD"""

        # Vemos si rootpath existe
        if not self.rootpath:
            self.get_root_path(tardir)
        if not self.adObject:
            self.failreason = format_reason(self.cuenta, reason.NODNINAD, "---", self.parent.stats)
            return False
        try:
            ad_file = open(self.rootpath + '/' + self.cuenta + '.dn', 'w')
            pickle.dump(self.adObject, ad_file)
            ad_file.close()
            return True
        except BaseException:
            self.failreason = format_reason(self.cuenta, reason.FAILARCHIVEDN, self.dn, self.parent.stats)
            return False

    def delete_dn(self):
        """Borra el dn del usuario"""

        # Dependiendo del tiempo transcurrido en el archivado, puede haberse superado
        # el timeout de la conexión y haberse cerrado. En este caso se presentará la
        # excepción ldap.SERVER_DOWN. Deberemos reabrir la conexión y reintentar el borrado

        # import ldap
        _print(1, 'Borrando DN: ', self.dn)
        global ldapCon

        if self.dn is not None:
            try:
                if DRYRUN:
                    return True
                ldapCon.delete_s(self.dn)
                return True
            except ldap.SERVER_DOWN:
                # La conexión se ha cerrado por timeout, reabrimos
                open_ldap(True)
                if config.status.ldapCon is True:
                    try:
                        ldapCon.delete_s(self.dn)
                        return True
                    except ldap.LDAPError, error:
                        _print(0, "Error borrando DN usuario despues de reconexion ", self.cuenta, " ", error)
                        self.failreason = format_reason(self.cuenta, reason.FAILDELETEDN, self.dn, self.parent.stats)
            except ldap.LDAPError, error:
                _print(0, "Error borrando DN usuario ", self.cuenta, " ", error)
                self.failreason = format_reason(self.cuenta, reason.FAILDELETEDN, self.dn, self.parent.stats)
            return False

    def insert_dn(self, tardir):
        """Como no es posible recuperar el SID no tiene sentido usarla"""
        import ldap

        if not self.rootpath:
            self.get_root_path(tardir)

        if self.dn is None:
            try:
                ad_file = open(self.rootpath + '/' + self.cuenta + '.dn', 'r')
                self.adObject = pickle.load(ad_file)

                item = self.adObject[0]
                dn = item[0]
                atributos = item[1]
                if DEBUG:
                    debug("DEBUG-INFO: (user.insert_dn) DN:", dn)
                    debug("DEBUG-INFO: (user.insert_dn) AT:", atributos)

                attrs = []
                attrlist = ["cn", "countryCode", "objectClass", "userPrincipalName", "info", "name", "displayName",
                            "givenName", "sAMAccountName"]
                for attr in atributos:
                    if attr in attrlist:
                        attrs.append((attr, atributos[attr]))

                if DEBUG:
                    debug("DEBUG-INFO: (user.insert_dn) ==== ATTRS ====", attrs)
                if not DRYRUN:
                    ldapCon.add_s(dn, attrs)
                ad_file.close()
                return True
            except ldap.LDAPError, error:
                _print(0, error)
                return False


import cmd


class Shell(cmd.Cmd):

    @staticmethod
    def parse(line):
        return line.split()

    def do_consolidate(self,line):
        """
        Consolida una sesión anterior con resume
        """
        global WINDOWS_PASS
        WINDOWS_PASS = "dummy"
        check_environment()


    def do_count(self, line):
        """
        Devuelve el numero de usuarios entre dos fechas
        <count fromDate toDate>
        Si tenemos fromfile no hace falta meter las dos fechas
        """
        global WINDOWS_PASS
        WINDOWS_PASS = "dummy"
        check_environment()

        if FROMFILE is not None:
            userlist = []
            ret = from_file(userlist)
            if not ret:
                print "Error recuperando la cuenta de usuarios de ", FROMFILE
            else:
                print "Usuarios de ", FROMFILE, " archivables = ", len(userlist)
            return
        else:
            try:
                _fromDate, _toDate = self.parse(line)
                userlist = get_list_by_date(_toDate, _fromDate)
            except BaseException, error:
                print "Error recuperando la cuenta de usuarios de SIGU: ", error
                return
            print "Usuarios archivables entre ", _fromDate, " y ", _toDate, " = ", len(userlist)

    @staticmethod
    def do_isarchived(line):
        """
        Devuelve si una cuenta tiene estado archivado
        <isarchived usuario>
        """
        try:
            global WINDOWS_PASS
            WINDOWS_PASS = "dummy"
            check_environment()
            print is_archived(line)
        except BaseException, error:
            print "Error recuperando el estado de archivado", error

    @staticmethod
    def do_hascuentant(line):
        """
        Comprueba si un usuario tiene cuenta NT
        <hascuentant usuario>
        """
        try:
            global WINDOWS_PASS
            global ORACLE_PASS
            if NTCHECK == 'ad':
                ORACLE_PASS = "dummy"
            if NTCHECK == 'sigu':
                WINDOWS_PASS = 'dummy'
            check_environment()
            print has_cuenta_nt(line), " (metodo ", NTCHECK, ")"
        except BaseException, error:
            print "Error comprobando si ", line, " tiene cuenta NT", error

    def do_ldapquery(self, line):
        """
        Consulta un atributo de ldap para una cuenta dada
        <ldapquery usuario atributo>
        """
        try:
            global WINDOWS_PASS
            WINDOWS_PASS = "dummy"
            check_environment()
            user, attr = self.parse(line)
            ret = ldap_from_sigu(user, attr)
            print ret
        except BaseException, error:
            print "Error consultando atributo ldap", error

    @staticmethod
    def do_schacquery(line):
        """Consulta el atributo schacUserStatus de ldap
        schacquery <usuario>"""
        global WINDOWS_PASS
        WINDOWS_PASS = "dummy"
        check_environment()
        ret = scha_from_ldap(line)
        print ret

    @staticmethod
    def do_allservicesoff(line):
        """Comprueba si todos los servicios de un usuario estan a off
        allservicesoff <usuario>"""
        global WINDOWS_PASS
        WINDOWS_PASS = "dummy"
        check_environment()
        print all_services_off(line)

    def do_advsarchived(self, line):
        """
        Lista aquellos archivados que aun estan en AD
        <advsarchived fromDate toDate>
        En caso de no especificar las fechas, se toma todo el rango temporal
        """
        check_environment()

        try:
            if line == '':
                _fromDate = '1900-01-01'
                _toDate = '2099-01-01'
            else:
                _fromDate, _toDate = self.parse(line)
            userlist = get_archived_by_date(_toDate, _fromDate)
        except BaseException, error:
            print "Error recuperando lista de usuarios archivados de SIGU: ", error
            return

        global NTCHECK
        NTCHECK = 'ad'
        contador = 0
        # noinspection PyTypeChecker
        for user in userlist:
            if has_cuenta_nt(user):
                print user
                contador += 1
        print "Usuarios archivados entre ", _fromDate, " y ", _toDate, " = ", len(userlist)
        print "Usuarios archivados que aun tienen cuenta AD: ", contador

    def do_archived(self, line):
        """
        Lista aquellos archivados entre dos fechas (basada en su caducidad en ut_cuentas)
        <archived fromdate toDate>
        En caso de no especificar las fechas, se toma todo el rango temporal
        """
        check_environment()

        try:
            if line == '':
                _fromDate = '1900-01-01'
                _toDate = '2099-01-01'
            else:
                _fromDate, _toDate = self.parse(line)
            userlist = get_archived_by_date(_toDate, _fromDate)
        except BaseException, error:
            print "Error recuperando lista de usuarios archivados de SIGU: ", error
            return

        print "Usuarios archivados entre ", _fromDate, " y ", _toDate, " = ", len(userlist)
        # noinspection PyTypeChecker
        for user in userlist:
            print user

    @staticmethod
    def do_isexpired(line):
        """ Muestra si un usuario esta expirado
        isexpired <usuario>"""
        check_environment()
        print is_expired(line)

    @staticmethod
    def do_stats(line):
        """Devuelve estadisticas sobre el proceso de archivado
        stats"""
        _ = line
        check_environment()

        cursor = oracleCon.cursor()
        cursor.execute("select sum(nficheros) from ut_st_storage")
        nficheros = fetch_single(cursor)
        cursor.execute("select sum(nsize_original) from ut_st_storage")
        nsize_original = fetch_single(cursor)
        cursor.execute("select sum(nsize) from ut_st_storage")
        nsize = fetch_single(cursor)
        cursor.execute("select count(*) from ut_st_storage")
        ntars = fetch_single(cursor)
        cursor.execute("select count(*) from ut_st_sesion")
        nsesiones = fetch_single(cursor)
        cursor.execute("select count(distinct(ccuenta)) from ut_st_storage")
        narchivados = fetch_single(cursor)
        cursor.execute(
            "select nficheros,ccuenta from ut_st_storage where nficheros = (select max(nficheros) from ut_st_storage)")
        maxficheros, cuenta_maxficheros = fetch_single(cursor)
        cursor.execute("select nsize_original,ccuenta from ut_st_storage where nsize_original = \
                       (select max(nsize_original) from ut_st_storage)")
        maxsize_orig, cuenta_maxsize_orig = fetch_single(cursor)
        cursor.close()

        print "*********************************"
        print "*** ESTADISTICAS DE SIGUCLEAN ***"
        print "*********************************"
        print "\n"
        print "Sesiones:\t", nsesiones
        print "Archivados:\t", narchivados
        print "Numero tars:\t", ntars
        print "Ficheros:\t", nficheros
        print "Tamaño Orig:\t", size_to_human(nsize_original)
        print "Tamaño Arch:\t", size_to_human(nsize)
        print "Max ficheros:\t", maxficheros, "(", cuenta_maxficheros, ")"
        print "Max tamaño:\t", size_to_human(maxsize_orig), "(", cuenta_maxsize_orig, ")"

    @staticmethod
    def do_arcinfo(line):
        """Muestra información de archivado del usuario.
           arcinfo usuario"""
        from texttable import Texttable

        check_environment()

        cursor = oracleCon.cursor()
        cursor.execute("select * from ut_st_storage where ccuenta = " + comillas(line))
        rows = cursor.fetchall()

        if not rows:
            print "Usuario no archivado"
        else:
            table = Texttable()
            table.add_row(["TARNAME", "SIZE", "ORIGSIZE", "FILES"])
            for row in rows:
                table.add_row([os.path.basename(row[2]), size_to_human(row[3]), size_to_human(row[5]), row[6]])
            print table.draw()

    @staticmethod
    def do_sql(line):
        """Permite ejecutar una consulta sql directamente contra sigu
        sql <consulta>"""
        from texttable import Texttable

        check_environment()

        formato, line = line.split(" ", 1)

        if formato != "short" and formato != "long":
            print "Debe especificar el formato (short o long)"
        try:
            cursor = oracleCon.cursor()
            cursor.execute(line)
            rows = cursor.fetchall()
            col_names = []
            for i in range(0, len(cursor.description)):
                col_names.append(cursor.description[i][0])
        except BaseException, error:
            print "ERROR: ", error
            return

        if not rows:
            print "No results"
        else:
            if formato == "short":
                table = Texttable()
            else:
                table = Texttable(0)

            table.header(col_names)
            # table.set_deco(Texttable.BORDER | Texttable.HLINES | Texttable.VLINES)
            table.add_rows(rows, header=False)
            print table.draw()

    @staticmethod
    def do_ignorearchived(line):
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
            except BaseException:
                print "Valor booleano incorrecto"

    @staticmethod
    def do_checkaltdir(line):
        """Chequea y ofrece estadisticas de directorios alt para un directorio raiz dado
        checkaltdir <directorio>"""
        from collections import defaultdict

        _f = None
        check_environment()
        dictdir = defaultdict(list)
        sumuserlist = 0
        parentdir = line + "/"
        altdirs = [s for s in os.listdir(parentdir)
                   if s.startswith(ALTROOTPREFIX)]

        for altdir in altdirs:
            userlist = os.listdir(parentdir + altdir)
            sumuserlist += len(userlist)
            for user in userlist:
                dictdir[user].append(altdir)

        fm = open("/tmp/multi-movidos", "w")
        fs = open("/tmp/single-movidos", "w")

        for k, v in dictdir.iteritems():
            if len(v) > 1:
                _f = fm
            else:
                _f = fs

            _f.write(k)
            if has_archived_data(k):
                _f.write("\t" + "_ARC_")
            else:
                _f.write("\t" + "NOARC")

            for value in v:
                _f.write("\t" + value)
            _f.write("\n")
        _f.close()
        print "LENDICT: ", len(dictdir)
        print "SUMLIST: ", sumuserlist

    @staticmethod
    def do_quit(line):
        print "Hasta luego Lucas ...."
        _ = line
        os._exit(True)

    def __init__(self):
        cmd.Cmd.__init__(self)


"""
Programa principal
"""
import argparse

parser = argparse.ArgumentParser(description='Siguclean 0.1: Utilidad para borrar storages de usuarios',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-n', '--sessionname', help='Nombre de sesion', dest='sessionId', action='store', default=None)
parser.add_argument('-p', '--altrootprefix', help='Prefijo para carpetas alternativas', dest='ALTROOTPREFIX',
                    action='store', default=None)
parser.add_argument('-i', '--interactive', help='Iniciar sesion interactiva', action='store_true')
parser.add_argument('-v', '--version', help='Mostrar la versión del programa', action='store_true')
parser.add_argument('-a', '--abortalways', help='Abortar siempre ante un error inesperado', dest='ABORTALWAYS',
                    action='store_true', default='False')
parser.add_argument('-d', '--abortdecrease',
                    help='Decrementar la cuenta de errores cuando se produzca un exito en el archivado',
                    dest='ABORTDECREASE', action='store_true', default='False')
parser.add_argument('-s', '--abortinseverity', help='Abortar si se produce un error con severidad',
                    dest='ABORTINSEVERITY', action='store_true', default='False')
parser.add_argument('-l', '--abortlimit', help='Limite de la cuenta de errores para abortar (0 para no abortar nunca)',
                    dest='ABORTLIMIT', action='store', default='5')
parser.add_argument('-f', '--from', help='Seleccionar usuarios desde esta fecha', dest='fromDate', action='store',
                    default=None)
parser.add_argument('-t', '--to', help='Seleccionar usuarios hasta esta fecha', dest='toDate', action='store',
                    default=None)
parser.add_argument('--ignore-archived', help='Excluye de la selección las cuentas con estado archivado',
                    dest='IGNOREARCHIVED', action='store_true', default='False')
parser.add_argument('-m', '--maxsize', help='Limite de tamaño del archivado (0 sin limite)', dest='MAXSIZE',
                    action='store', default='0')
parser.add_argument('-w', '--windows-check', help='Metodo de comprobacion de existencia de cuenta windows',
                    choices=['ad', 'sigu', 'both'], dest='NTCHECK', action='store', default='sigu')
parser.add_argument('--win-password', help='Clave del administrador de windows', dest='WINDOWS_PASS', action='store',
                    default=None)
parser.add_argument('--sigu-password', help='Clave del usuario sigu', dest='ORACLE_PASS', action='store', default=None)
parser.add_argument('--test', help='Para usar solo en el periodo de pruebas', dest='TEST', action='store_true')
parser.add_argument('--debug', help='Imprimir mensajes de depuración', dest='DEBUG', action='store_true')
parser.add_argument('--dry-run', help='No realiza ninguna operación de escritura', dest='DRYRUN', action='store_true')
parser.add_argument('--soft-run', help='Junto a dry-run, si genera los tars y la inserción en la BBDD', dest='SOFTRUN',
                    action='store_true')
parser.add_argument('-v', help='Incrementa el detalle de los mensajes', dest='verbosity', action='count')
parser.add_argument('--progress', help='Muestra indicación del progreso', dest='PROGRESS', action='store_true')
parser.add_argument('-x', '--mount-exclude', help='Excluye esta regex de los posibles montajes', dest='MOUNT_EXCLUDE',
                    action='store', default="(?=a)b")
parser.add_argument('--confirm', help='Pide confirmación antes de realizar determinadas acciones', dest='CONFIRM',
                    action='store_true')
parser.add_argument('--fromfile', help='Nombre de fichero de entrada con usuarios', dest='FROMFILE', action='store',
                    default=None)
parser.add_argument('--sessiondir', help='Carpeta para almacenar la sesion', dest='TARDIR', action='store',
                    default=None)
parser.add_argument('--restore', help='Restaura la sesion especificada', dest='RESTORE', action='store_true')
parser.add_argument('--restoring', help='Opción interna para una sesion que esta restaurando una anterior. No usar.',
                    dest='RESTORING', action='store', default=False)
parser.add_argument('--consolidate', help='Consolida la sesion especificada', dest='CONSOLIDATE', action='store_true')
parser.add_argument('--exclude-userfile', help='Excluir los usuarios del fichero parámetro', dest='EXCLUDEUSERSFILE',
                    action='store', default=None)
parser.add_argument('--mandatory-relax', help='Nivel de chequeo de storages mandatory', dest='MANDATORYRELAX',
                    action='store', default=Mrelax.NONE)
parser.add_argument('--ldap-relax', help='No tiene en cuenta si el usuario está o no en ldap', dest='LDAPRELAX',
                    action='store_true')
args = parser.parse_args()

_ = VERBOSE
VERBOSE = args.verbosity
# NOTA: Los debugs previos a la creación de la sesión no se pueden almacenar en el fichero
# debug pues aun no hemos establecido la rotación de logs

if DEBUG and not RESTORE:
    debug('verbose es: ', VERBOSE)

# Si no es interactiva ponemos los valores a las globales
for var in args.__dict__:
    if var in globals().keys():
        if vars(args)[var] is not None:
            if args.DEBUG:
                debug('DEBUG-INFO: existe ', var, ' y es ', vars(args)[var])
            globals()[var] = vars(args)[var]

if args.interactive:
    Shell().cmdloop()
    os._exit(True)

if args.version:
    print __version__

if DEBUG and not RESTORE and not CONSOLIDATE:
    debug('DEBUG-INFO: SessionId: ', sessionId, ' Fromdate: ', fromDate,
          ' toDate: ', toDate, ' Abortalways: ', ABORTALWAYS, ' Verbose ',
          VERBOSE)

cmdline = sys.argv
cmdlinestr = ' '.join(cmdline)
sesion = None
try:
    sesion = Session(sessionId, fromDate, toDate)
except BaseException, e:
    _print(0, 'ABORT: Error en la creación de la sesion')
    _print(0, "ERROR: ", e)
    os._exit(False)

# Guardamos los argumentos
# Si no es una sesión restore salvamos el string
if not RESTORE:
    f = open(sesion.logsdir + "/cmdline", "w")
    f.write(cmdlinestr + "\n")
    f.close()
else:
    # Leemos la linea de comando anterior y le añadimos --ignore-archived si no lo tenía
    # Siempre trabajaremos sobre la linea de comando original del directorio logs
    _print(0, "... Restaurando sesion anterior ...")
    # Leemos la linea de comando
    f = open(sesion.logsdir + "/cmdline", "r")
    cmdlinestr = f.readline().rstrip('\n')
    f.close()
    # Leemos el idsesion anterior
    f = open(sesion.logsdir + "/idsesion", "r")
    oldidsesion = f.readline().rstrip('\n')
    f.close()

    if "--ignore-archived" not in cmdlinestr:
        cmdlinestr += " --ignore-archived"
    if " --restoring" not in cmdlinestr:
        cmdlinestr = cmdlinestr + " --restoring " + oldidsesion
    # Lanzamos el subproceso
    p = subprocess.Popen(cmdlinestr, shell=True)
    p.wait()
    os._exit(True)

check_environment()

if not CONSOLIDATE:
    sesion.start()
else:
    # Estamos en una sesion de consolidacion. Algunas comprobaciones previas
    _print(0, "... Consolidando sesion anterior ...")
    # Comprobamos que existe la sesion y leemos el id
    if not sesion.get_session_id():
        os._exit(False)
    if DEBUG:
        debug("DEBUG-INFO: Idsession es ", sesion.idsesion)
    # Generamos la lista de usuarios a partir de los ya archivados
    if not sesion.account_list_from_current():
        os._exit(False)
    if DEBUG:
        debug("DEBUG-INFO: Recuperada lista usuarios desde FS. Numero usuarios: ", len(sesion.userList))
    sesion.consolidate_fs('homenfs')
    sesion.consolidate_fs('homemail')
    sesion.consolidate_fs('perfiles')
    sesion.consolidate_fs('homecifs')
    sesion.start()
os._exit(True)