#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""

from enum import Enum
import re
import cx_Oracle
from enum import Enum
import os
import datetime
from progressbar import *
import pickle

import config
from sc_funcs import *
from sc_funcs import _print
from sc_log   import *


#Clases Enum
state = Enum('NA', 'ARCHIVED', 'DELETED', 'TARFAIL', 'NOACCESIBLE', 'ROLLBACK', 'ERROR', 'DELETEERROR', 'UNARCHIVED',
             'NOTARCHIVABLE','LINKERROR')


class Session(object):
    """Clase para procesar una sesion de archivado"""
    def __init__(self, session_id, from_date, to_date):
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
        self.abortLimit = config.ABORTLIMIT
        self.abortDecrease = config.ABORTDECREASE
        self.abortAlways = config.ABORTALWAYS
        self.abortInSeverity = config.ABORTINSEVERITY
        self.idsesion = None
        self.logsdir = ''

        #print globals()
        #global TARDIR
        # Comprobamos los parametros para poder ejecutar
        if not self.sessionId:
            raise ValueError
        if not self.fromDate:
            self.fromDate = '1900-01-01'
        if not self.toDate:
            self.toDate = '2100-01-01'
        # Comprobamos que existe TARDIR
        print "TARDIR ES: ",config.TARDIR
        print "WINDOWS_PASS ES:",config.WINDOWS_PASS
        if config.TARDIR is None:
            raise Exception("No ha dado valor a sessiondir")
        # Directorio para los tars
        if os.path.exists(config.TARDIR):
            if self.sessionId:
                self.tardir = config.TARDIR + '/' + self.sessionId
            if not os.path.isdir(self.tardir):
                os.mkdir(self.tardir, 0777)
        else:
            # Abortamos porque no existe el directorio padre de los tars
            _print(0, 'ABORT: (session-start) No existe el directorio para tars: ', config.TARDIR)
            os._exit(False)
        self.log = Log(self)
        self.stats = Stats(self)
        # Tratamos MAXSIZE
        # Intentamos convertir MAXSIZE a entero
        try:
            a = int(config.MAXSIZE)
            config.MAXSIZE = a
            if config.DEBUG is True:
                debug("MAXSIZE era un entero y vale ", config.MAXSIZE)
        except BaseException:
            # Es una cadena vemos si es auto, convertible de humano o devolvemos error
            if config.MAXSIZE == "auto":
                try:
                    statfs = os.statvfs(config.TARDIR)
                    config.MAXSIZE = int(statfs.f_bsize * statfs.f_bfree * 0.9)
                    if config.DEBUG:
                        debug("MAXSIZE era auto y vale ", config.MAXSIZE)
                except BaseException:
                    _print(0, "ABORT: Calculando MAXSIZE para ", config.TARDIR)
                    os._exit(False)
            else:
                a = human_to_size(config.MAXSIZE)
                if a is not False:
                    config.MAXSIZE = a
                    if config.DEBUG:
                        debug("MAXSIZE era sizehuman y vale ", config.MAXSIZE)
                else:
                    _print(0, "ABORT: opción MAXSIZE invalida: ", config.MAXSIZE)
                    os._exit(False)

        # Tratamos el fichero EXCLUDEUSERSFILE
        if config.EXCLUDEUSERSFILE is not None:
            _print(0, "Excluyendo usuarios de ", config.EXCLUDEUSERSFILE)
            if os.path.exists(config.EXCLUDEUSERSFILE):
                try:
                    _f = open(config.EXCLUDEUSERSFILE, "r")
                    # Leemos los usuarios quitando el \n final
                    self.excludeuserslist.extend([line.strip() for line in _f])
                    _f.close()
                except BaseException, error:
                    if config.DEBUG:
                        debug("Error leyendo EXCLUDEUSERSFILE: ", error)
                    _print(0, "Error leyendo EXCLUDEUSERSFILE: ", config.FROMFILE)
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
            cursor = config.oracleCon.cursor()
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
        print "Lineas: ", lineas, " DoneDict: ", len(usersdonedict)

        usersfaileddict, lineas = self.logdict(logs_dirs, 'users.failed')
        print "Lineas: ", lineas, " FailedDict: ", len(usersfaileddict)

        userslistdict, lineas = self.logdict(logs_dirs, 'users.list')
        print "Lineas: ", lineas, " ListDict: ", len(userslistdict)

        usersrollbackdict, lineas = self.logdict(logs_dirs, 'users.rollback')
        print "Lineas: ", lineas, " RollbackDict: ", len(usersrollbackdict)

        ppp = set(usersrollbackdict).difference(set(usersdonedict))
        print "DifRollbackLen: ", len(ppp)
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
        if config.DEBUG:
            debug("DEBUG: Archives len es: ", len(archives))

        # Vemos la diferencia entre uno y otro, estos serán los que no se han procesado.
        diff = set(origin_dict.keys()).difference(set(archives))
        if config.DEBUG:
            debug("DEBUG: La diferencia entre origenes y archives es: ", len(diff))
        # Abrimos el cursor
        try:
            cursor = config.oracleCon.cursor()
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
                if not config.DRYRUN:
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
                    if config.DEBUG:
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

            if not oneshot and config.ONESHOT:
                confirm()
                oneshot = True

        if not config.DRYRUN:
            cursor.close()
            config.oracleCon.commit()

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
        from collections import defaultdict

        origindict = defaultdict(str)

        # Buscamos en mounts la base
        regex = re.compile(config.MOUNT_EXCLUDE)
        # Desactivamos temporalmente DEBUG
        tmp_debug = config.DEBUG
        config.DEBUG = False
        for mount in config.MOUNTS:
            if mount['fs'] == fs:
                raiz = get_mount_point(mount['label'], regex)
        # Recuperamos el valor de DEBUG
        config.DEBUG = tmp_debug

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

        if config.EXTRADEBUG:
            debug("EXTRADEBUG-INFO: ABORTALWAYS ES: ", config.ABORTALWAYS)
        if config.ABORTLIMIT == 0:
            _print(0, 'ABORT: No abortamos porque ABORTLIMIT es 0')
            return

        if config.ABORTALWAYS is True:
            _print(0, 'ABORT: Error y ABORTALWAYS es True')
            os._exit(False)

        if config.ABORTINSEVERITY is True and severity is True:
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
                if config.DEBUG:
                    debug("DEBUG-INFO: Rollback exitoso de ", user.cuenta)
                self.log.write_rollback(user.cuenta)
                self.abort(False)
            else:
                if config.DEBUG:
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
        if config.TEST:
            self.accountList = ['games', 'news', 'uucp', 'pepe']
            return True
        else:
            if config.FROMFILE is not None:
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
        if config.DEBUG:
            debug("DEBUG-INFO: (session-bbdd_insert) RESTORING es: ", config.RESTORING)
        if not config.RESTORING and not config.CONSOLIDATE:
            try:
                cursor = config.oracleCon.cursor()
                # Consigo el idsesion
                self.idsesion = int(cursor.callfunc('UF_ST_SESION', cx_Oracle.NUMBER))
                values = values_list(self.idsesion, now.strftime('%Y-%m-%d'), self.fromDate,
                                     self.toDate, self.sessionId)
                query = config.Q_INSERT_SESION % values
                self.log.write_bbdd(query)
                if config.DRYRUN and not config.SOFTRUN:
                    return True
                cursor.execute(query)
                config.oracleCon.commit()
                cursor.close()
                # Salvamos en idsesion para restoring
                if config.DEBUG:
                    debug("DEBUG-INFO: (session-bbdd_insert) salvando idsesion: ", self.idsesion)
            except BaseException, error:
                _print(0, "ERROR: Almacenando en la BBDD sesion ", self.sessionId)
                if config.DEBUG:
                    debug("DEBUG-ERROR: (sesion.bbdd_insert) Error: ", error)
                return False
        else:
            # Leemos el valor de la sesion en curso
            self.idsesion = int(self.get_session_id())
            if config.DEBUG:
                debug("DEBUG-INFO: Leido ID sesion: ", self.idsesion)

        _f = open(self.logsdir + "/idsesion", "w")
        _f.write(str(self.idsesion) + "\n")
        _f.close()
        return True

    def start(self):
        # Directorio para TARS
        pbar = None
        if config.DEBUG:
            debug('DEBUG-INFO: (session.start) TARDIR es: ', config.TARDIR)
        print "VERBOSE: ", config.VERBOSE, "DEBUG: ", config.DEBUG, "PROGRESS: ", config.PROGRESS
        if have_progress():
            pbar = ProgressBar(widgets=[Percentage(), " ", Bar(marker=RotatingMarker()), " ", ETA()],
                               maxval=1000000).start()
        if not config.CONSOLIDATE:
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
        #Ajustamos el progress segun el tipo de procesamiento
        #En dryrun es solo una estimacion
        if config.DRYRUN:
            if len(self.accountList)>100:
                ppFirstPhase = 1
                ipFirstPhase = 100000
                ppSecondPhase = ppFirstPhase + ipFirstPhase
                ipSecondPhase = 1000000 - ppSecondPhase
            else:
                ppFirstPhase = 1
                ipFirstPhase = 200000
                ppSecondPhase = ppFirstPhase + ipFirstPhase
                ipSecondPhase = 1000000 - ppSecondPhase
        else:
            #Para mas de 10 usuarios la primera fase es despreciable
            if len(self.accountList)>100:
                ppFirstPhase = 1
                ipFirstPhase = 10
                ppSecondPhase = ppFirstPhase + ipFirstPhase
                ipSecondPhase = 1000000 - ppSecondPhase
            else:
                ppFirstPhase = 1
                ipFirstPhase = 10000
                ppSecondPhase = ppFirstPhase + ipFirstPhase
                ipSecondPhase = 1000000 - ppSecondPhase


        if have_progress():
            pbar.update(ppFirstPhase)
        # Creo la lista de objetos usuario a partir de la lista de cuentas
        pp = ppFirstPhase
        ip = ipFirstPhase / len(self.accountList)
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
            pbar.update(ppSecondPhase)
        # Insertamos sesion en BBDD
        self.bbdd_insert()
        # Proceso las entradas
        skip = False
        pp = ppSecondPhase
        ip = ipSecondPhase / len(self.userList)

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
            if not config.PROGRESS:
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
            if 'WINDOWS' in user.cuentas and not config.CONSOLIDATE:
                # Si falla el archivado de DN continuamos pues quiere decir que no está en AD
                # Si ha hecho el archivado y falla el borrado, hacemos rollback
                if not user.archive_dn(self.tardir):
                    if config.DEBUG:
                        debug("DEBUG-WARNING: Error archivando DN de ", user.cuenta)
                    if not self.die(user, False):
                        pass
                        # continue
                else:
                    if not user.delete_dn():
                        if config.DEBUG:
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
            if config.ABORTDECREASE:
                self.abortCount -= 1
            if self.abortCount < 0:
                self.abortCount = 0
            if config.DEBUG:
                debug('DEBUG-INFO: (session.start) abortCount: ' + str(self.abortCount))
            self.log.write_done(user.cuenta)
            # Controlamos si hemos llegado al tamaño maximo
            if config.MAXSIZE > 0:
                if self.tarsizes > config.MAXSIZE:
                    skip = True
        _print(1, 'Tamaño de tars de la session ', self.sessionId, ' es ', size_to_human(self.tarsizes))
        self.stats.fin = datetime.datetime.now()
        self.stats.show()

class Storage(object):
    def __init__(self, key, path, link, mandatory, parent):
        if config.TRACE:
            traceprint(self.__class__.__name__+":"+current_func(),parent.__class__.__name__+":"+current_parent())

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
        if config.CONSOLIDATE:
            self.mandatory = False
        else:
            self.mandatory = mandatory
            # Superseed de MANDATORYRELAX
        if config.MANDATORYRELAX == config.Mrelax.TODOS or (config.MANDATORYRELAX == config.Mrelax.CANCELADOS and self.parent.cEstado == config.CANCELADO):
            self.mandatory = False

    def display(self):
        _print(1, self.key, "\t = ", self.path, "\t Accesible: ", self.accesible, "\t Estado: ", self.state)

    def archive(self, rootpath):
        """ Archiva un storage en un tar"""
        # Vuelvo a comprobar aqui que es accesible
        if not self.accesible:
            self.state = state.NOACCESIBLE
            return False
        self.tarpath = rootpath + '/' + self.parent.cuenta + '@' + self.key + '@' + config.sessionId + ".tar.bz2"
        _print(1, "Archivando ", self.key, " from ", self.path, " in ", self.tarpath, " ... ")
        try:
            if config.DRYRUN and not config.SOFTRUN:
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
            if config.DEBUG:
                debug("DEBUG-ERROR: ", error)
            self.state = state.TARFAIL
            return False

    def bbdd_insert(self, cursor):
        """ Inserta un registro de archivado en la BBDD """
        # Solo procesamos si el storage se completo y por tanto esta en estado deleted
        if config.EXTRADEBUG:
            debug("EXTRADEBUG-INFO: (storage-bbdd_insert) self.state: ", self.state, " key: ", self.key)
        if self.state == state.DELETED:
            try:
                # Como en sigu
                values = values_list(config.session.idsesion, self.parent.cuenta, self.tarpath, self.tarsize,
                                     self.state._index, self.size_orig, self.files)
                query = config.Q_INSERT_STORAGE % values
                config.session.log.write_bbdd(query)
                if config.DRYRUN and not config.SOFTRUN:
                    return True
                cursor.execute(query)
                # Hago el commit en el nivel superior
                # oracleCon.commit()
                # cursor.close()
                return True
            except BaseException, error:
                _print(0, "ERROR: Almacenando en la BBDD storage ", self.key)
                if config.DEBUG:
                    debug("DEBUG-ERROR: (storage.bbdd_insert) Error: ", error)
                return False
        else:
            # Si no estaba archivado y no cascó lo consideramos correcto
            return True

    def delete(self):
        """Borra un storage"""
        # Primero tengo que controlar si no existe y no es mandatory
        if config.DEBUG:
            debug("DEBUG-INFO: (storage.delete) ", self.key, " en ", self.path)
        if not self.accesible and not self.mandatory:
            self.state = state.NOACCESIBLE
            return True
        try:
            if config.DRYRUN:
                self.state = state.DELETED
                return True
            rmtree(self.path)
            if self.link is not None:
                os.remove(self.link)
            self.state = state.DELETED
            return True
        except BaseException, error:
            if config.DEBUG:
                debug("DEBUG_ERROR: Borrando ", self.path, " : ", error)
            self.state = state.DELETEERROR
            return False

    def rollback(self):
        """Deshace el archivado borra los tars.
        - Si se ha borrado hacemos un untar
        - Borramos el tar
        - Ponemos el state como rollback"""
        if config.EXTRADEBUG:
            debug('EXTRADEBUG-INFO: (storage.rollback)', self.__dict__)
        if self.state in (state.DELETED, state.DELETEERROR):
            if not self.unarchive():
                self.state = state.ERROR
                return False
            # Restauro el link si existe
            if self.link is not None:
                if not config.DRYRUN:
                    try:
                        os.link(self.path, self.link)
                    except BaseException, error:
                        if config.DEBUG:
                            debug("DEBUG_ERROR: Restableciendo link ", self.link, " a ",self.path," : ", error)
                        self.state = state.LINKERROR
                        return False
        try:
            # Si no está archivado no hay que borrar el tar
            if self.state not in (state.ARCHIVED, state.TARFAIL, state.UNARCHIVED):
                if config.DEBUG:
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
                if config.DRYRUN:
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
        if config.DEBUG:
            debug("DEBUG-INFO: ****** PROCESANDO ", self.path, " *******")
        if os.path.exists(self.path):
            self.accesible = True
            return True
        # Aun no existiendo puede estar en un directorio movido, lo buscamos
        parent_dir = os.path.dirname(self.path)
        basename = os.path.basename(self.path)
        # Nos aseguramos de que si ya hemos buscado y no hay alternativos salir
        if parent_dir in config.altdirs and not config.altdirs[parent_dir]:
            if config.DEBUG:
                debug("DEBUG-WARNING: User:", self.parent.cuenta, " No existe path directo ni alternativo para ",
                      self.key, " en ", parent_dir)
            self.accesible = False
            return False
        if config.DEBUG:
            debug("DEBUG-INFO: User:", self.parent.cuenta, " No existe path directo para ", self.key,
                  " en ", self.path, " busco alternativo ...")
        # Buscamos en directorios alternativos del parent_dir
        # esta busqueda puede ser gravosa si se debe repetir para cada usuario por
        # lo que una vez averiguados los alternativos se deben de almacenar globalmente
        if parent_dir not in config.altdirs:
            if config.DEBUG:
                debug("DEBUG-INFO: No he construido aun la lista alternativa para ", self.key,
                      " en ", parent_dir, " lo hago ahora ...")
            config.altdirs[parent_dir] = [s for s in os.listdir(parent_dir)
                                          if s.startswith(config.ALTROOTPREFIX)]
        # Si la lista esta vacia salimos directamente
        if not config.altdirs[parent_dir]:
            if config.DEBUG:
                debug("DEBUG-WARNING: No existen directorios alternativos para ", self.key, " en ", parent_dir)
            self.accesible = False
            return False
        # Buscamos si existe en cualquiera de los directorios alternativos
        for path in config.altdirs[parent_dir]:
            joinpath = os.path.join(parent_dir, path, basename)
            if os.path.exists(joinpath):
                if config.DEBUG:
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

        if direct_path and config.CONSOLIDATE:
            if config.DEBUG:
                debug("DEBUG-INFO: INCONSISTENCIA, el usuario ", self.parent.cuenta,
                      " ha resucitado, no proceso ", self.key)
            direct_path = False

        # Como no hay path directo ni ubicaciones alternativas para este tipo de storage
        # salimos  de la funcion retornando false.

        if not direct_path and parent_dir in config.altdirs and not config.altdirs[parent_dir]:
            if config.DEBUG:
                debug("DEBUG-WARNING: User:", self.parent.cuenta, " No existe path directo ni alternativo para ",
                      self.originalkey, " en ", parent_dir)
            self.accesible = False
            return False

        # Buscamos en directorios alternativos del parent_dir
        # esta busqueda puede ser gravosa si se debe repetir para cada usuario por
        # lo que una vez averiguados los alternativos se deben de almacenar globalmente

        if parent_dir not in config.altdirs:
            if config.DEBUG:
                debug("DEBUG-INFO: No he construido aun la lista alternativa para ", self.originalkey, " en ",
                      parent_dir, " lo hago ahora ...")
            config.altdirs[parent_dir] = [s for s in os.listdir(parent_dir)
                                          if s.startswith(config.ALTROOTPREFIX)]

            # Comprobamos el directo
        if direct_path:
            if config.DEBUG:
                debug("DEBUG-INFO: Encontrado path directo para ", self.originalkey)
            self.accesible = True
            self.directstorage = True
            first_path = False

        # Existen ubicaciones alternativas?
        if config.altdirs[parent_dir]:
            if config.DEBUG:
                debug("DEBUG-INFO: Existen ubicaciones alternativas de ", self.originalkey, " cuenta: ",
                      self.parent.cuenta)

            # Buscamos si existe en cualquiera de los directorios alternativos
            for path in config.altdirs[parent_dir]:
                joinpath = os.path.join(parent_dir, path, basename)
                if os.path.exists(joinpath):
                    if config.DEBUG:
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
            if config.DEBUG:
                debug("DEBUG-INFO: No existen ubicaciones alternativas de ", self.originalkey, " cuenta: ",
                      self.parent.cuenta)

        if not self.morestoragelist:
            if config.DEBUG:
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
        if not config.PROGRESS and config.EXTRADEBUG:
            print "ESTADO USUARIO: ", self.cEstado
            print "MRELAX: ", config.MANDATORYRELAX, "TIPO: ", type(config.MANDATORYRELAX)
            print "CANCELADO ES: ", config.CANCELADO
            print "mRelax.TODOS: ", config.Mrelax.TODOS

        if not all_services_off(self.cuenta):
            self.exclude = True
            if config.DEBUG:
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
        if not config.CONSOLIDATE:
            if archived is True:
                status = False
                self.failreason = format_reason(self.cuenta, reason.ISARCHIVED, "---", self.parent.stats)
                if config.DEBUG:
                    debug("DEBUG-WARNING: (user.check) El usuario ", self.cuenta, " ya estaba archivado")
                self.exclude = True
                return status
            elif archived is None:
                status = False
                self.failreason = format_reason(self.cuenta, reason.UNKNOWNARCHIVED, "---", self.parent.stats)
                if config.DEBUG:
                    debug("DEBUG-ERROR: (user.check) Error al comprobar estado de archivado de ", self.cuenta)
                self.exclude = True
                return status
            elif archived is not False:
                status = False
                self.failreason = format_reason(self.cuenta, reason.NOTARCHIVABLE, "---", archived)
                if config.DEBUG:
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
                if config.DEBUG:
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
        cursor = config.oracleCon.cursor()
        for storage in self.storage:
            ret = storage.bbdd_insert(cursor)
            if not ret:
                # Debo hacer un rollback
                if config.DEBUG:
                    debug("DEBUG-ERROR: (user-bbdd_insert) Insertando: ", storage.key)
                config.oracleCon.rollback()
                cursor.close()
                self.failreason = format_reason(self.cuenta, reason.INSERTBBDDSTORAGE, storage.key, self.parent.stats)
                return False
        # Debo hacer un commit
        config.oracleCon.commit()
        cursor.close()
        return True

    def borra_cuenta_windows(self):
        # TODO
        """Borra la cuenta windows de la BBDD sigu
        Pendiente de implementar"""
        _ = self
        return True

    def list_cuentas(self):
        if config.TRACE:
            traceprint(self.__class__.__name__+":"+current_func(),self.parent.__class__.__name__+":"+current_parent())

        """Devuelve una tupla con las cuentas que tiene el usuario
        Por defecto tenemos correo y linux, para ver si tenemos windows
        consultamos si existe en la tabla UT_CUENTAS_NT, en AD o en ambos"""

        if config.TEST:
            return 'LINUX', 'MAIL'  # dummy return

        if has_cuenta_nt(self.cuenta):
            return "LINUX", "MAIL", "WINDOWS"
        else:
            return "LINUX", "MAIL"

    def estado(self):
        if config.TRACE:
            traceprint(self.__class__.__name__+":"+current_func(),self.parent.__class__.__name__+":"+current_parent())

        q_cestado = "select cestado from ut_cuentas where ccuenta = %s" % comillas(self.cuenta)
        cursor = config.oracleCon.cursor()
        cursor.execute(q_cestado)
        return fetch_single(cursor)

    def __init__(self, cuenta, parent):
        if config.TRACE:
            traceprint(self.__class__.__name__+":"+current_func(),parent.__class__.__name__+":"+current_parent())

        try:
            _ = self.cuenta
            if config.DEBUG:
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

        try:
            self.homedir = os.path.basename(ldap_from_sigu(cuenta, 'homedirectory'))
        except BaseException:
            if config.LDAPRELAX:
                self.homedir = cuenta
            else:
                self.failreason = reason.NOTINLDAP
                self.exclude = True
                return

        self.storage = []
        self.rootpath = ''
        self.cuentas = self.list_cuentas()
        pase_por_aqui = False
        for c in self.cuentas:
            # relleno el diccionario storage
            for m in config.MOUNTS:
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
                            _print(1, "El usuario ", self.cuenta, " no tiene DN en AD y debería tenerla")
                            self.dn = False
                        if config.DEBUG and not pase_por_aqui:
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
                if config.DEBUG:
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
                if config.DEBUG:
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
        except BaseException, error:
            self.failreason = format_reason(self.cuenta, reason.FAILARCHIVEDN, self.dn, self.parent.stats)
            debug("DEBUG-INFO: (archive_dn) error ",error)
            return False

    def delete_dn(self):
        """Borra el dn del usuario"""

        # Dependiendo del tiempo transcurrido en el archivado, puede haberse superado
        # el timeout de la conexión y haberse cerrado. En este caso se presentará la
        # excepción ldap.SERVER_DOWN. Deberemos reabrir la conexión y reintentar el borrado

        # import ldap
        _print(1, 'Borrando DN: ', self.dn)

        if self.dn is not None:
            try:
                if config.DRYRUN:
                    return True
                config.ldapCon.delete_s(self.dn)
                return True
            except ldap.SERVER_DOWN:
                # La conexión se ha cerrado por timeout, reabrimos
                open_ldap(True)
                if config.status.ldapCon is True:
                    try:
                        config.ldapCon.delete_s(self.dn)
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
                if config.DEBUG:
                    debug("DEBUG-INFO: (user.insert_dn) DN:", dn)
                    debug("DEBUG-INFO: (user.insert_dn) AT:", atributos)

                attrs = []
                attrlist = ["cn", "countryCode", "objectClass", "userPrincipalName", "info", "name", "displayName",
                            "givenName", "sAMAccountName"]
                for attr in atributos:
                    if attr in attrlist:
                        attrs.append((attr, atributos[attr]))

                if config.DEBUG:
                    debug("DEBUG-INFO: (user.insert_dn) ==== ATTRS ====", attrs)
                if not config.DRYRUN:
                    config.ldapCon.add_s(dn, attrs)
                ad_file.close()
                return True
            except ldap.LDAPError, error:
                _print(0, error)
                return False

    def accesible_storages(self):
        i=0
        for _storage in self.storage:
            if _storage.accesible_now():
                i += 1
        return i

    def get_storage_list(self):
        #devuelve una lista de strings con los storages
        storage_list = []
        for _storage in self.storage:
            storage_list.append([_storage.key,_storage.path,("True" if _storage.accesible else "False"),_storage.state])
        return storage_list