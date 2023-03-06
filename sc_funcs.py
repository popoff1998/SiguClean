#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""

from enum import Enum
import re
import contextlib
import os
import datetime
import inspect
from pprint import pprint
import dateutil.parser
import collections
import tarfile
from tenacity import *


import config

def current_func():
    return inspect.stack()[1][3]

def current_parent():
    return inspect.stack()[2][3]

def traceprint(current,parent):
    print "TRACE: current: ",current," parent: ",parent

def fetch_single(cursor):
    if config.TRACE:
        traceprint(current_func(),current_parent())

    ret = cursor.fetchone()
    if ret is not None:
        if len(ret) == 1:
            ret = ret[0]
    return ret


@contextlib.contextmanager
def cd_change(tmp):
    """Funcion para cambiar temporalmente a un directorio y volver al anterior despues"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    cd = os.getcwd()
    os.chdir(tmp)
    try:
        yield
    finally:
        os.chdir(cd)


def have_progress():
    """Comprueba si se dan las condiciones de mostrar la barra de progreso"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    if config.PROGRESS and config.VERBOSE == 0:
        return True
    else:
        return False


def confirm():
    """Pide confirmacion por teclado"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    a = raw_input("Desea continuar S/N (N)")
    if a == "S":
        return True
    else:
        exit(True)


def iterable(obj):
    """Comprueba si un objeto es iterable"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    if isinstance(obj, collections.Iterable):
        return True
    return False


def _pprint(*_args):
    """Imprime de forma bonita algo, teniendo en cuenta si es iterable o no"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    for arg in _args:
        if iterable(arg):
            pprint(arg),
        else:
            print(arg),


def check_environment():
    """Chequea el entorno de ejecucion (instancia unica)"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    _print(1, "PASO1: Comprobando el entorno de ejecucion ...")
    if not config.CHECKED:
        check_modules()
        check_connections()
        check_mounts()
    config.CHECKED = True


def check_modules():
    """Comprueba que son importables los módulos ldap y cx_Oracle"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

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
    if config.TRACE:
        traceprint(current_func(),current_parent())

    if reconnect:
        verb = "DEBUG-WARNING: Reabriendo"
    else:
        verb = "Abriendo"

    if config.DEBUG:
        debug(verb, " conexión a ldap")
    try:
        ldap.set_option(ldap.OPT_X_TLS_REQUIRE_CERT, 0)
        ldap.set_option(ldap.OPT_REFERRALS, 0)
        config.ldapCon = ldap.initialize(config.LDAP_SERVER)
        config.ldapCon.simple_bind_s(config.BIND_DN, config.WINDOWS_PASS)
        config.status.ldapCon = True
    except ldap.LDAPError, _e:
        _print(1, "ERROR: ", verb, " conexión a ldap")
        _print(_e)
        config.status.ldapCon = False


def check_connections():
    """Establece las conexiones a ldap y oracle"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    _print(1, "  Comprobando conexiones")

    checkLdapConnection()
    checkOracleConnection()

def checkLdapConnection():
    from pyssword import pyssword

    # LDAP
    if not config.WINDOWS_PASS:
        config.WINDOWS_PASS = pyssword('     Introduzca la clave de windows (administrador): ')
    if config.WINDOWS_PASS != "dummy":
        _print(1, '     comprobando conexion a ldap ... ', end='')
        open_ldap(False)
        if config.status.ldapCon is True:
            _print(1, "CORRECTO")


def checkOracleConnection():
    import cx_Oracle
    from pyssword import pyssword

    # Oracle
    if not config.ORACLE_PASS:
        config.ORACLE_PASS = pyssword('     Introduzca la clave de oracle (sigu): ')
    if config.ORACLE_PASS != "dummy":
        _print(1, '     comprobando conexion a oracle ... ', end='')
        try:
            config.oracleCon = cx_Oracle.connect('sigu/' + config.ORACLE_PASS + '@' + config.ORACLE_SERVER)
            config.status.oracleCon = True
            _print(1, "CORRECTO")
        except cx_Oracle.DatabaseError:
            _print(1, "ERROR")
            config.status.oracleCon = False

def get_mount_point(algo, exclude_regex):
    """Devuelve el punto de montaje que contiene algo en el export"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    try:
        with open("/proc/mounts", "r") as ifp:
            for line in ifp:
                fields = line.rstrip('\n').split()
                if config.EXTRADEBUG:
                    debug("DEBUG-INFO: EXPORT: ", fields[0], " MOUNT: ", fields[1], " ALGO: ", algo)
                if algo in fields[0]:
                    # Es un posible montaje, vemos si esta excluido
                    ret = exclude_regex.search(fields[1])
                    if config.DEBUG:
                        debug("DEBUG-INFO: (get_mount_point): campo es ", fields[1], " ret es ", ret)
                    if ret is not None:
                        if config.DEBUG:
                            debug("DEBUG-INFO: EXCLUIDO")
                        pass
                    else:
                        if config.DEBUG:
                            debug("DEBUG-INFO: INCLUIDO")
                        return fields[1]
    except EnvironmentError:
        pass
    return None  # explicit


def check_mounts():
    """Comprueba que los puntos de montaje están accesibles"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    _print(1, "  Comprobando el acceso a los Datos")
    try:
        regex = re.compile(config.MOUNT_EXCLUDE)
        if config.DEBUG:
            debug("DEBUG-INFO: Regex de exclusion es ", config.MOUNT_EXCLUDE, " y su valor es ", regex)
    except:
        _print(0, "ABORT: La expresion ", config.MOUNT_EXCLUDE, " no es una regex valida, abortamos ...")
        regex = None
        os._exit(False)
    salgo = False
    for mount in config.MOUNTS:
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
    if config.CONFIRM:
        verblevel = 0
    else:
        verblevel = 1
    _print(verblevel, "*** RESUMEN DE MONTAJES ***")
    for mount in config.MOUNTS:
        if len(mount['label']) < 8:
            tabs = "\t\t\t"
        else:
            tabs = "\t\t"
        _print(verblevel, mount['label'], tabs, mount['val'])
    if config.CONFIRM:
        confirm()


def input_parameter(param, text, mandatory):
    """Lee un parametro admitiendo que la tecla intro ponga el anterior"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

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
    if config.TRACE:
        traceprint(current_func(),current_parent())

    while True:
        print "PASO2: Parametros de la sesion ('c' para borrar)"
        config.sessionId = input_parameter(config.sessionId, "Identificador de sesion: ", True)
        config.fromDate = input_parameter(config.fromDate, "Fecha desde (yyyy-mm-dd): ", False)
        config.toDate = input_parameter(config.toDate, "Fecha hasta (yyyy-mm-dd): ", True)

        print '\nSessionId = [' + config.sessionId + ']'
        print 'fromDate = [' + config.fromDate + ']'
        print 'toDate = [' + config.toDate + ']'

        sal = raw_input('\nSon Correctos (S/n): ')
        if sal == 'S':
            return
        else:
            continue


def pager(_iterable, page_size):
    """Funcion paginador"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    import itertools

    _args = [iter(_iterable)] * page_size
    fill_value = object()
    for group in itertools.izip_longest(fillvalue=fill_value, *_args):
        yield (elem for elem in group if elem is not fill_value)


def imprime(user_list):
    """Imprime usando un paginador"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    my_pager = pager(user_list, 20)
    for page in my_pager:
        for i in page:
            _print(1, i)
        tecla = raw_input("----- Pulse intro para continuar (q para salir) ----------")
        if tecla == 'q':
            break


def size_to_human(size):
    """Convierte un numero de bytes a formato humano"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    symbols = ('B', 'K', 'M', 'G', 'T')
    indice = 0
    #if config.EXTRADEBUG: debug("EXTRADEBUG-INFO: (size_to_human) Size antes de redondear es: ",size )
    while True:
        if size < 1024:
            string = str(round(size, 1)) + " " + symbols[indice]
            return string
        size /= 1024.0
        indice += 1


def human_to_size(size):
    """Convierte un tamaño en formato humano a bytes"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

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
        if config.DEBUG:
            debug('DEBUG-ERROR: (human_to_size) ', size, ' no es traducible')
        return False


def time_stamp():
    """Devuelve un timestamp"""

    return '[{0}]\t'.format(str(datetime.datetime.now()))


def _print(level, *_args, **kwargs):
    """Formatea y archiva los mensajes por pantalla"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    if not config.VERBOSE:
        config.VERBOSE = 0
    if kwargs != {}:
        trail = kwargs['end']
    else:
        trail = '\n'
    cadena = "".join(str(x) for x in _args)
    if config.VERBOSE >= level:
        print cadena + trail,

    if config.session:
        if hasattr(config.session, 'log'):
            config.session.log.write_log(cadena + trail, False)
            config.session.log.write_all_output(cadena + trail, False)


def debug(*_args, **kwargs):
    """Formatea y archiva los mensajes de debug"""

    # Si tenemos verbose o no tenemos sesion sacamos la info por consola tambien
    if config.VERBOSE > 0 or not config.session:
        print "".join(str(x) for x in _args)
    # Si tenemos definido el log de la sesion lo grabamos en el fichero, en caso
    # contrario solo salen por pantalla
    # En sesiones restore no abrimos el fichero
    if config.session and not config.RESTORE:
        if not config.fDebug:
            config.fDebug = open(config.session.logsdir + "/debug", "w")

        if kwargs != {}:
            trail = kwargs['end']
        else:
            trail = '\n'
        config.fDebug.write(time_stamp())
        if config.fAllOutput is not None:
            config.fAllOutput.write(time_stamp())
        for string in _args:
            config.fDebug.write(str(string))
            if config.fAllOutput is not None:
                config.fAllOutput.write(str(string))
        config.fDebug.write(trail)
        if config.fAllOutput is not None:
            config.fAllOutput.write(trail)
        config.fDebug.flush()
        if config.fAllOutput is not None:
            config.fAllOutput.flush()


def dn_from_user(user):
    """Devuelve la DN de un usuario de active directory"""
    if config.TRACE:
        traceprint(current_func(),current_parent())


    import ldap
    dn = tupla = result_type = None
    filtro = "(&(CN=" + user + ")(!(objectClass=contact)))"
    try:
        result_id = config.ldapCon.search(config.USER_BASE,
                                   ldap.SCOPE_SUBTREE,
                                   filtro,
                                   None)
        result_type, tupla = config.ldapCon.result(result_id, 1)
        dn, none = tupla[0]
        _status = True
    except:
        _status = False
    return _status, dn, tupla, result_type

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))

def ldap_from_sigu(cuenta, attr):
    """Consulta un atributo ldap mediante sigu"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    # La funcion uf_leeldap busca en people o people-deleted segun el valor
    # del tercer parametro (que sea B o no), asi que hacemos una busqueda en
    # el primero y despues en el segundo en caso de que no encontremos nada

    #PRUEBA GORDA: Cerramos y reabrimos la conexion con sigu para evitar problema con +63 conexiones abiertas
    #Parece que hay un pufo con la fución uf_leeldap que en determinadas circunstancias deja las conexiones ldap abiertas
    if config.oracleCon and config.CLOSEORACLEONLDAP:
        config.oracleCon.close()
        config.oracleCon = cx_Oracle.connect('sigu/' + config.ORACLE_PASS + '@' + config.ORACLE_SERVER)
    elif not config.oracleCon:
        config.oracleCon = cx_Oracle.connect('sigu/' + config.ORACLE_PASS + '@' + config.ORACLE_SERVER)

    q_ldap_sigu = 'select uf_leeldap(' + comillas(cuenta) + ',' + comillas(attr) + ') from dual'

    try:
        cursor = bbdd_execute(q_ldap_sigu)
        tmp_list = cursor.fetchall()
        cursor.close()
    except BaseException, error:
        print "ERROR: en ldap_from_sigu (1)"
        raise Exception("Retry")
        return False
    tmp_list = tmp_list[0][0]
    if config.EXTRADEBUG:
        if tmp_list:
            debug("DEBUG-INFO (ldapFromSigu1): ", cuenta, " tmp_list = ", tmp_list.strip())
    if tmp_list is not None:
        return tmp_list.strip().split(':')[1].strip()
    # Hacemos la comprobacion en people-deleted
    q_ldap_sigu = 'select uf_leeldap(' + comillas(cuenta) + ',' + comillas(attr) + ',' + comillas('B') + ') from dual'
    try:
        cursor = bbdd_execute(q_ldap_sigu)
        tmp_list = cursor.fetchall()
        cursor.close()
    except BaseException, error:
        print "ERROR: en ldap_from_sigu (2)"
        raise Exception("Retry")
        return False
    tmp_list = tmp_list[0][0]
    if config.EXTRADEBUG:
        debug("DEBUG-INFO (ldapFromSigu2): ", cuenta, " tmp_list = ", tmp_list)
    return tmp_list.strip().split(':')[1].strip() if tmp_list else None


def scha_from_ldap(cuenta):
    """Devuelve un diccionario con los permisos de la cuenta"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    q_ldap_sigu = 'select cServicio,tServicio,uf_valida_servicio_ldap(' + comillas(
        cuenta) + ',cServicio) from ut_servicios_mapa'
    # q_ldap_sigu = 'select sigu.ldap.uf_leeldap(\''+cuenta+'\',\'schacuserstatus\') from dual'
    cursor = bbdd_execute(q_ldap_sigu)
    tmp_list = cursor.fetchall()
    #pprint(tmp_list)
    cursor.close()

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
    if config.TRACE:
        traceprint(current_func(),current_parent())

    if len(services) < config.NUMSCHASERVICES:
        return len(services)
    for i in services:
        if services[i] != 'N' and i in config.OFFSERVICES:
            return False
    return True


def all_services_off(user):
    if config.TRACE:
        traceprint(current_func(),current_parent())

    services = scha_from_ldap(user)
    # Si no devuelve nada, no tiene ningun servicio a off
    if services is None:
        return False
    else:
        ret = ldap_from_sigu(user,'uid')
        if  ret is None:
            if config.DEBUG:
                debug("El usuario ",user," no existe en ldap")
            #El usuario no existe en LDAP
            return True
        else:
            return are_services_off(services)


def check_services(user_list):
    if config.TRACE:
        traceprint(current_func(),current_parent())

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
    if config.TRACE:
        traceprint(current_func(),current_parent())

    q_between_dates = 'FCADUCIDAD  BETWEEN to_date(\'' + from_date + \
                      '\',\'yyyy-mm-dd\') AND to_date(\'' + to_date + \
                      '\',\'yyyy-mm-dd\')'
    query = '{0} AND {1}'.format(config.Q_GET_BORRABLES, q_between_dates)
    if config.IGNOREARCHIVED is True:
        _print(1, 'INFO: Ignorando los ya archivados')
        query = '{0} AND {1}'.format(query, config.Q_IGNORE_ARCHIVED)
    if config.DEBUG:
        debug("DEBUG-INFO: (get_list_by_date) Query:", query)
    try:
        cursor = bbdd_execute(query)
        tmp_list = cursor.fetchall()
        cursor.close()
    except BaseException, error:
        _print(0, "ERROR: Error recuperando la lista de usuarios")
        if config.DEBUG:
            debug("DEBUG-ERROR: (get_list_by_date): ", error)
        return None
    # Convertimos para quitar tuplas
    user_list = [x[0] for x in tmp_list]
    config.status.userList = True
    return user_list


def get_archived_by_date(to_date, from_date='1900-01-01'):
    """Devuelve una lista de cuentas entre dos fechas"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    q_between_dates = 'FCADUCIDAD  BETWEEN to_date(\'' + from_date + \
                      '\',\'yyyy-mm-dd\') AND to_date(\'' + to_date + \
                      '\',\'yyyy-mm-dd\')'
    query = '{0} AND {1} AND {2}'.format(config.Q_GET_BORRABLES, q_between_dates, config.Q_ONLY_ARCHIVED)
    if config.DEBUG:
        debug("DEBUG-INFO: (get_list_by_date) Query:", query)
    try:
        cursor = bbdd_execute(query)
        tmp_list = cursor.fetchall()
        cursor.close()
    except BaseException, error:
        _print(0, "ERROR: Error recuperando la lista de usuarios archivados")
        if config.DEBUG:
            debug("DEBUG-ERROR: (get_list_by_date): ", error)
        return None
    # Convertimos para quitar tuplas
    user_list = [x[0] for x in tmp_list]
    config.status.userList = True
    return user_list

def get_unarchived_by_date(to_date, from_date='1900-01-01'):
    """Devuelve una lista de cuentas entre dos fechas"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    q_between_dates = 'FCADUCIDAD  BETWEEN to_date(\'' + from_date + \
                      '\',\'yyyy-mm-dd\') AND to_date(\'' + to_date + \
                      '\',\'yyyy-mm-dd\')'
    query = '{0} AND {1} AND {2}'.format(config.Q_GET_BORRABLES, q_between_dates, config.Q_IGNORE_ARCHIVED)
    if config.DEBUG:
        debug("DEBUG-INFO: (get_list_by_date) Query:", query)
    try:
        cursor = bbdd_execute(query)
        tmp_list = cursor.fetchall()
        cursor.close()
    except BaseException, error:
        _print(0, "ERROR: Error recuperando la lista de usuarios no archivados")
        if config.DEBUG:
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
    if config.TRACE:
        traceprint(current_func(),current_parent())


    #TODO: La funcion UF_ST_ULTIMA no devuelve 9 para los que no estan caducados
    try:
        # Llamo a la función uf_st_ultima
        cursor,ret = bbdd_callfunc('UF_ST_ULTIMA_TONIN', cx_Oracle.STRING, [user])
        _print(2,"USER: ",user," RET_UF_ST_ULTIMA_TONIN: ",ret)
        cursor.close()
        if ret == "0":
            #Chequeamos si aun estando marcado como archivado no tiene ficheros
            if config.CHECKARCHIVEDDATA and not has_archived_data(user):
                if config.DEBUG:
                    debug("DEBUG-WARNING: (is_archived) archivado pero sin ficheros ", user )
                return False
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
        if config.DEBUG:
            debug("DEBUG-ERROR: (is_archived) ", error)
        return None


def is_expired(user):
    """Comprueba si un usuario esta expirado (caducado o cancelado)"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    try:
        cursor = bbdd_execute("select CESTADO from ut_cuentas where CCUENTA = " + comillas(user))
        ret = fetch_single(cursor)
        cursor.close()
    except BaseException, error:
        _print(0, "ERROR: Consultando estado archivable del usuario ", user)
        _print(0, "ERROR-CODE: ", error)
        return None

    if ret == "":
        return None
    if ret == '4' or ret == '6':
        return True
    else:
        return False


def has_archived_data(user):
    """Devuelve True si hay algún registro de archivado en ut_st_storage"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    try:
        cursor = bbdd_execute("select unique ccuenta from ut_st_storage where ccuenta = " + comillas(user))
        ret = fetch_single(cursor)
        cursor.close()
        if ret == "":
            return False
        else:
            return True
    except:
        return False


def has_cuenta_nt(cuenta):
    """Comprueba si un usuario tiene cuenta NT"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    import ldap

    stat_ad = False
    stat_sigu = False

    if config.NTCHECK == 'sigu' or config.NTCHECK == 'both':
        query = config.Q_GET_CUENTA_NT % comillas(cuenta)
        cursor = bbdd_execute(query)
        stat_sigu = cursor.fetchall()
        cursor.close()

    if config.NTCHECK == 'ad' or config.NTCHECK == 'both':
        filtro = "(&(CN=" + cuenta + ")(!(objectClass=contact)))"
        try:
            result_id = config.ldapCon.search(config.USER_BASE,
                                       ldap.SCOPE_SUBTREE,
                                       filtro,
                                       None)
            result_type, tupla = config.ldapCon.result(result_id, 1)
            if len(tupla) == 1:
                stat_ad = False
            else:
                stat_ad = True
        except:
            stat_ad = False

    if config.NTCHECK == 'sigu':
        return True if stat_sigu else False
    if config.NTCHECK == 'ad':
        return stat_ad
    return True if stat_ad and stat_sigu else False


def comillas(cadena):
    """ Nos devuelve una cadena entre comillas"""

    return '\'' + cadena + '\''


def values_list(*_args, **kwargs):
    """Devuelve una cadena con una serie de valores formateados para sentencia sql"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    _ = kwargs
    cadena = '('
    size = len(_args)
    arg_number = 1
    date_pattern = re.compile("....-..-..")

    for x in _args:
        if type(x) is str:
            if date_pattern.match(x):
                # Es una fecha parseable
                cad = 'TO_DATE(' + comillas(x) + ',\'YYYY-MM-DD\')'
            else:
                cad = comillas(x)
        else:
            cad = str(x)
        cadena += cad
        if arg_number < size:
            cadena += ','
        arg_number += 1
    cadena += ')'
    return cadena


def format_reason(user, _reason, attr, stats):
    """Formatea la razon de fallo devolviendo una cadena"""

    stats.reason[_reason._index] += 1
    return user + "\t" + _reason._key + "\t" + attr


def filter_archived(user_list):
    """Filtra de una lista de usuarios dejando solo los que no estan archivados
    descontando los que dan otro tipo de salida que no sea False"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    user_list[:] = [x for x in user_list if is_archived(x) is False]


def from_file(user_list):
    """Lee la lista desde un fichero, teniendo en cuenta el filtro de exclusión"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    if os.path.exists(config.FROMFILE):
        try:
            _f = open(config.FROMFILE, "r")
            # Leemos los usuarios quitando el \n final
            user_list.extend([line.strip() for line in _f])
            _f.close()
            # Si tenemos IGNOREARCHIVED filtramos la lista
            if config.IGNOREARCHIVED is True:
                filter_archived(user_list)
                if config.EXTRADEBUG:
                    debug("EXTRADEBUG-INFO: Lista filtrada: ", user_list)
            return True
        except BaseException, error:
            if config.DEBUG:
                debug("Error leyendo FROMFILE: ", error)
            _print(0, "Error leyendo FROMFILE: ", config.FROMFILE)
            return False
    else:
        _print(0, "El fichero FROMFILE ", config.FROMFILE, " no existe")
        return False


def unique_name(filename):
    """Devuelve un nombre unico para un fichero que se va a renombrar"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    contador = 0
    while os.path.exists(filename + "." + str(contador)):
        contador += 1
    return filename + "." + str(contador)


def display_table(header,lista):
    """ Muestra una tabla a partir de una lista"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    from texttable import Texttable

    trows,tcols = os.popen('stty size','r').read().split()
    table = Texttable(int(tcols)-1)

    table.header(header)
    # table.set_deco(Texttable.BORDER | Texttable.HLINES | Texttable.VLINES)
    table.add_rows(lista, header=False)

    print table.draw()


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
        self.manualdelete = 0
        self.inicio = datetime.datetime.now()
        self.fin = None
        self.reason = [0] * len(config.reason)

    def show(self):
        _print(0, "-------------------------------")
        _print(0, "ESTADISTICAS DE LA SESION")
        _print(0, "-------------------------------")
        _print(0, "Total:\t\t", self.total)
        _print(0, "Correctos:\t", self.correctos)
        _print(0, "Incorrectos:\t", self.total - self.correctos)
        if config.MANUALDELETE:
            _print(0, "ManualDelete:\t",self.manualdelete," storages")

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
            _print(0, config.reason[i], ":\t", r)
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

def open_tofile():
    if config.TRACE:
        traceprint(current_func(),current_parent())

    try:
        config.TOFILEHANDLE = open(config.TOFILE, 'w')
        return True
    except BaseException,error:
        print "Error abriendo tofile en ",config.TOFILE
        print "ERROR: ",error
        return False

def write_tofile(line):
    if config.TRACE:
        traceprint(current_func(),current_parent())

    try:
        config.TOFILEHANDLE.writelines(line+"\n")
        config.TOFILEHANDLE.flush()
        return True
    except BaseException,error:
        print "Error escribiendo en ",config.TOFILE
        print "ERROR: ",error
        return False

def close_tofile():
    if config.TRACE:
        traceprint(current_func(),current_parent())

    try:
        config.TOFILEHANDLE.close()
        config.TOFILEHANDLE = None
        return True
    except BaseException,error:
        print "Error cerrando ",config.TOFILE
        print "ERROR: ",error
        return False

def parse_arctar(tarname):
    """
    Parsea un tarname para extraer el path de destino
    """
    if config.TRACE:
        traceprint(current_func(),current_parent())

    #excluimos el DUMMY
    if tarname.startswith('dummy'):
        return None

    pattern = re.compile("^([^@]*)@([^=@]*)=*([^@]*)@(.*)\.tar\.bz2")
    fields = pattern.split(tarname)

    #for field in fields:
    #    print field
    return fields

def unarchive_tar(tarname,destdir,force):
    """Des-archiva un tar"""
    if config.TRACE:
        traceprint(current_func(),current_parent())

    if not os.path.isdir(destdir):
        try:
            os.makedirs(destdir)
        except BaseException, error:
            _print(0,"Error creando directorio destino para unarchive")
            return False
    elif not force:
            _print(0,"Existe el directorio ",destdir," y no especificó -f")
            return False

    _print(0, "Desarchivando ", tarname, " to ", destdir)
    try:
        tar = tarfile.open(tarname, "r:*")
        tar.extractall(destdir)
        tar.close()
        return True
    except BaseException,error:
        _print(0, "Error extrayendo tar de ", tarname, " a ", destdir)
        _print(0,"ERROR: ", error)
        return False

def delete_bbdd_storage(idsesion, ccuenta, ttar):
    if config.TRACE:
        traceprint(current_func(),current_parent())

    try:
        query = "delete from ut_st_storage where  CCUENTA =" + comillas(ccuenta) + " AND IDSESION = " + str(idsesion) + " AND TTAR=" + comillas(ttar)
        cursor = bbdd_execute(query)
        #Este commit inmediato no debe dar problemas
        config.oracleCon.commit()
        cursor.close()
        _print(0,"Borrado en la bbdd el storage de ",ccuenta)
        return True
    except BaseException, error:
        _print(0,"Error al borrar en la bbdd el storage de ",ccuenta)
        _print(0,"QUERY: ",query)
        _print(0,"ERROR: ",error)
        return False

def bbdd_execute(sql):
    """Ejecuta una sentencia sql de forma atomica"""
    reintentos=2
    for intento in range(reintentos):
        try:
            cursor = config.oracleCon.cursor()
            cursor.execute(sql)
            return cursor
        except cx_Oracle.InterfaceError as e:
            #Hay un error de conexion, reconectamos y reintentamos
            _print(0,"Error en la conexion a la bbdd, reintento ",intento+1)
            checkOracleConnection()
    raise e

def bbdd_callfunc(func,type,list):
    reintentos=2
    for intento in range(reintentos):
        try:
            cursor = config.oracleCon.cursor()
            ret = cursor.callfunc(func,type,list)
            return cursor,ret
        except cx_Oracle.InterfaceError as e:
            #Hay un error de conexion, reconectamos y reintentamos
            _print(0,"Error en la conexion a la bbdd, reintento ",intento+1)
            checkOracleConnection()
    raise e

def bbdd_commit():
    reintentos=2
    for intento in range(reintentos):
        try:
            config.oracleCon.commit()
        except cx_Oracle.InterfaceError as e:
            #Hay un error de conexion, reconectamos y reintentamos
            _print(0,"Error en la conexion a la bbdd, reintento ",intento+1)
            checkOracleConnection()
    raise e

def get_sessions_by_user(user):
    """Devuelve las sesiones en las que un usuario tiene archivados"""

    check_environment()

    query = "select DISTINCT a.idsesion,b.dsesion from ut_st_storage A, ut_st_sesion B where ccuenta= " + comillas(user) + " and a.idsesion=b.idsesion"
    cursor = bbdd_execute(query)
    rows = cursor.fetchall()

    return rows

def has_cuenta_sigu(user):
    """Comprueba si un usuario tiene cuenta en sigu"""

    check_environment()

    query = "select ccuenta from ut_cuentas where ccuenta =" + comillas(user)
    cursor = bbdd_execute(query)
    row = fetch_single(cursor)

    if row:
        return row
    else:
        return False

def has_cuenta_ldap(user):
    """Comprueba si un usuario tiene cuenta en ldap"""

    ret=ldap_from_sigu(user,'uid')
    if  ret is None:
        return False
    else:
        return ret
