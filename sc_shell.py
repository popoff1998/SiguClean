#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
import readline
import cmd
import ast
from pprint import pprint
import datetime

import config
from sc_funcs import *
from sc_classes import *

STRING = 0
LIST = 1

class Shell(cmd.Cmd):

    @staticmethod
    def parse(line):
        return line.split()

    @staticmethod
    def parse_with_args(line,valid_options,type):
        args=[]
        _line=[]

        for item in line.split():
            if item.startswith('-'):
                if item not in valid_options:
                    print "Argumento ",item," inválido"
                    return None,None
                args.append(item)
            else:
                _line.append(item)
        if type == STRING:
            _line = " ".join(str(x) for x in _line)
        return args,_line

    def do_consolidate(self,line):
        """
        Consolida una sesión anterior con resume
        """
        config.WINDOWS_PASS = "dummy"
        check_environment()


    def do_count(self, line):
        """
        Devuelve el numero de usuarios entre dos fechas

        count <fromDate toDate>

        Si tenemos fromfile no hace falta meter las dos fechas
        """
        config.WINDOWS_PASS = "dummy"
        check_environment()

        if config.FROMFILE is not None:
            userlist = []
            ret = from_file(userlist)
            if not ret:
                print "Error recuperando la cuenta de usuarios de ", config.FROMFILE
            else:
                print "Usuarios de ", config.FROMFILE, " archivables = ", len(userlist)
            return
        else:
            try:
                _fromDate, _toDate = self.parse(line)
                userlist = get_list_by_date(_toDate, _fromDate)
            except BaseException, error:
                print "Error recuperando la cuenta de usuarios de SIGU: ", error
                return
            print "Usuarios archivables entre ", _fromDate, " y ", _toDate, " = ", len(userlist)

    def do_storages(self,line):
        """
        Muestra los storages actuales de un usuario.
        La opción -l muestra los mismos.

        storages [-l] <usuario1> <usuario2> ...
        """
        import gc
        config.WINDOWS_PASS = "dummy"
        check_environment()

        args,accounts = self.parse_with_args(line,['-l'],LIST)
        if not accounts:
            return

        #Ponemos las variables para asegurar la ejecución completa de la función user::check()
        _LDAPRELAX = config.LDAPRELAX
        _BYPASS = config.BYPASS
        _MANDATORYRELAX = config.MANDATORYRELAX
        config.MANDATORYRELAX = config.Mrelax.TODOS
        config.BYPASS = True
        config.LDAPRELAX = True

        #Ajustamos atributos para simular una sesion
        self.excludeuserslist = []
        self.stats = Stats(self)

        header = ["KEY","PATH","ACCESIBLE","ESTADO"]
        for account in accounts:
            user = User(account,self)
            user.check()
            if not user.cEstado:
                print account," no existe"
                del user
                continue
            accesible_storages = user.accesible_storages()

            if accesible_storages > 0:
                if '-l' not in args:
                    print account,"\t",accesible_storages
                else:
                        display_table(header,user.get_storage_list())
                        print "\n"
            else:
                print account," no tiene storages"
            del user
            gc.collect()
        try:
            if user:
                del user
        except:
            pass

        #Restauramos el valos de las variables.
        config.BYPASS = _BYPASS
        config.MANDATORYRELAX = _MANDATORYRELAX
        config.LDAPRELAX = _LDAPRELAX

    def do_checktrash(self, line):
        """
        Comprueba si existen restos de storages de usuarios que ya ha
        sido archivados en el rango temporal especificado.

        checktrash [-p] <fromdate todate>

        Si tenemos fromfile no hace falta poner las dos fechas. Si no lo
        tenemos y omitimos las fechas se aplica a todos los archivados.
        La opción -p muestra la barra de progreso
        """

        config.WINDOWS_PASS = "dummy"
        check_environment()

        #Ajustamos atributos para simular una sesion
        self.excludeuserslist = []
        self.stats = Stats(self)
        config.LDAPRELAX = True

        args,line = self.parse_with_args(line,['-p'],STRING)

        if '-p' in args:
            config.PROGRESS = True

        if config.FROMFILE is not None:
            userlist = []
            ret = from_file(userlist)
            if not ret:
                print "Error recuperando la cuenta de usuarios de ", config.FROMFILE
                return
            else:
                print "Usuarios de ", config.FROMFILE, " = ", len(userlist)
        else:
            try:
                _fromDate, _toDate = self.parse(line)
                userlist = get_archived_by_date(_toDate, _fromDate)
            except BaseException, error:
                print "Error recuperando la cuenta de usuarios de SIGU: ", error
                return
            print "Usuarios archivados entre ", _fromDate, " y ", _toDate, " = ", len(userlist)

        if len(userlist) == 0:
            return

        result_list = []
        founds=0
        strfounds="      "
        if have_progress():
            #TODO: Actualizar progressbar para poder poner los usuarios encontrados como DynamicMessage
            pbar = ProgressBar(widgets=[Percentage(), " ", Bar(marker=RotatingMarker()), " ", ETA()],
                               maxval=len(userlist)).start()

            pbar.update(0)
            pbar_index=0

        strfounds = str(founds)

        for account in userlist:
            if have_progress():
                pbar_index += 1
                pbar.update(pbar_index)
            user = User(account,self)
            user.check()
            user_storages = user.accesible_storages()
            if user_storages > 0:
                if have_progress():
                    result_list.append([account,user_storages])
                    founds +=1
                    strfounds = str(founds)
                else:
                    print "USER: ",account," ACCESIBLE_STORAGES: ",user_storages

        if have_progress():
            for result in result_list:
                print "USER: ",result[0]," ACCESIBLE_STORAGES: ",result[1]


    @staticmethod
    def do_isarchived(line):
        """
        Devuelve si una cuenta tiene estado archivado
        <isarchived usuario>
        """
        config.WINDOWS_PASS = "dummy"
        check_environment()

        try:
            print is_archived(line)
        except BaseException, error:
            print "Error recuperando el estado de archivado", error

    @staticmethod
    def do_hasarchiveddata(line):
        """
        Devuelve los archivados que tiene un usuario o False si no tiene
        Es independiente del flag isarchived, aunque lo muestra tambien
        <hasarchiveddata usuario>
        """
        try:
            config.WINDOWS_PASS = "dummy"
            check_environment()
            print "ISARCHIVED: ", is_archived(line)
            print "HASARCHIVEDDATA", has_archived_data(line)
        except BaseException, error:
            print "Error recuperando los datos de archivado", error

    @staticmethod
    def do_hascuentant(line):
        """
        Comprueba si un usuario tiene cuenta NT
        <hascuentant usuario>
        """
        try:
            if config.NTCHECK == 'ad':
                config.ORACLE_PASS = "dummy"
            if config.NTCHECK == 'sigu':
                config.WINDOWS_PASS = 'dummy'
            check_environment()
            print has_cuenta_nt(line), " (metodo ", config.NTCHECK, ")"
        except BaseException, error:
            print "Error comprobando si ", line, " tiene cuenta NT", error

    def do_ldapquery(self, line):
        """
        Consulta un atributo de ldap para una cuenta dada
        <ldapquery usuario atributo>
        """
        try:
            config.WINDOWS_PASS = "dummy"
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
        config.WINDOWS_PASS = "dummy"
        check_environment()
        ret = scha_from_ldap(line)
        print ret

    @staticmethod
    def do_allservicesoff(line):
        """Comprueba si todos los servicios de un usuario estan a off
        allservicesoff <usuario>"""
        config.WINDOWS_PASS = "dummy"
        check_environment()
        print all_services_off(line)

    def do_advsarchived(self, line):
        """
        Lista aquellos archivados que aun estan en AD

        advsarchived [-c|-l] <fromDate toDate>

        En caso de no especificar las fechas, se toma todo el rango temporal.
        La opción -c limita la busqueda a los archivados y caducados.
        la opción -l muestra el DN del usuario
        """
        #TODO: Implementarlo en una sola select de sql

        check_environment()

        args,line = self.parse_with_args(line,['-l','-c'],LIST)

        try:
            if not line:
                _fromDate = '1900-01-01'
                _toDate = '2099-01-01'
            else:
                _fromDate = line[0]
                _toDate = line[1]
            userlist = get_archived_by_date(_toDate, _fromDate)
        except BaseException, error:
            print "Error recuperando lista de usuarios archivados de SIGU: ", error
            return

        config.NTCHECK = 'ad'
        contador = 0
        # noinspection PyTypeChecker
        for user in userlist:
            if has_cuenta_nt(user):
                print user,
                if '-l' in args:
                    _status, dn, tupla, result_type = dn_from_user(user)
                    print "\t",dn
                else:
                    print
                contador += 1
        print "Usuarios archivados entre ", _fromDate, " y ", _toDate, " = ", len(userlist)
        print "Usuarios archivados que aun tienen cuenta AD: ", contador

    def do_archived(self, line):
        """
        Lista aquellos archivados entre dos fechas (basada en su caducidad en ut_cuentas)

        archived <fromdate toDate>

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

    def do_unarchived(self, line):
        """
        Lista aquellos no archivados entre dos fechas (basada en su caducidad en ut_cuentas)

        unarchived [-c] <fromdate toDate>

        En caso de no especificar las fechas, se toma todo el rango temporal.
        La opción -c chequea que el usuario realmente tiene storages (CHECKARCHIVEDDATA)
        """
        check_environment()

        args,line = self.parse_with_args(line,['-c'],STRING)

        if config.FROMFILE is not None:
            userlist = []
            ret = from_file(userlist)
            if not ret:
                print "Error recuperando la cuenta de usuarios de ", config.FROMFILE
            else:
                print "Usuarios de ", config.FROMFILE, " archivables = ", len(userlist)
            return
        else:
            try:
                if line == '':
                    _fromDate = '1900-01-01'
                    _toDate = '2099-01-01'
                else:
                    _fromDate, _toDate = self.parse(line)
                userlist = get_unarchived_by_date(_toDate, _fromDate)
            except BaseException, error:
                print "Error recuperando lista de usuarios no archivados de SIGU: ", error
                return

        if '-c' in args:
            _tmp = config.CHECKARCHIVEDDATA
            #Eliminamos los que no tienen storages
            for user in userlist:
                if not is_archived(user):
                    userlist.remove(user)
            config.CHECKARCHIVEDDATA = _tmp

        print "Usuarios no archivados entre ", _fromDate, " y ", _toDate, " = ", len(userlist)
        # noinspection PyTypeChecker
        for user in userlist:
            print user
            if config.TOFILEHANDLE:
                try:
                    write_tofile(user)
                except:
                    return

        if config.TOFILEHANDLE:
            try:
                close_tofile()
            except:
                return

    @staticmethod
    def do_isexpired(line):
        """ Muestra si un usuario esta expirado
        isexpired <usuario>"""
        check_environment()
        print is_expired(line)

    @staticmethod
    def do_stats(line):
        """Devuelve estadisticas sobre el proceso de archivado
        stats <*|descriptor|numero>

        Sin argumentos devuelve las estadísticas generales,
        Con * devuelve las estadísticas individuales de cada sesion,
        Con la descripción de la sesión o el número de la sesión devuelve solo las de esa sesión"""
        sesiones = []
        _ = line
        check_environment()
        if not line:
            cursor = config.oracleCon.cursor()
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

            compresion = 100-((nsize*100)/float(nsize_original))
            espaciobyusuario = nsize_original/narchivados

            print "\n"
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
            print "Tasa Compresion\t",'%.2f' % compresion,"%"
            print "Tamaño/user\t",size_to_human(espaciobyusuario)
            print "Max ficheros:\t", maxficheros, "(", cuenta_maxficheros, ")"
            print "Max tamaño:\t", size_to_human(maxsize_orig), "(", cuenta_maxsize_orig, ")"
            print "\n"
        else:
            if line == "*":
                try:
                    cursor = config.oracleCon.cursor()
                    cursor.execute("select IDSESION from ut_st_sesion")
                    sesiones = cursor.fetchall()
                    cursor.close()
                except:
                    print "Error recuperando todas las sesiones"
                    cursor.close()
                    return
            elif not line.isdigit():
                #el argumento es el descriptor de una sesion, conseguimos el idsesion
                cursor = config.oracleCon.cursor()
                try:
                    cursor.execute("select IDSESION from ut_st_sesion where DSESION = '" + line + "'")
                    sesiones = cursor.fetchall()
                    cursor.close()
                except BaseException, error:
                    print "Error recuperando sesion", line
                    print "ERROR: ", error
                    cursor.close()
                    return
                if not sesiones:
                    print "Error la sesion ",line," no existe"
            else:
                #El argumento es un numero de sesion
                sesiones.append((int(line),))

            #print "DEBUG: Sesiones = ",sesiones
            cursor = config.oracleCon.cursor()
            for sesiontuple in sesiones:
                sesion = sesiontuple[0]
                try:
                    cursor.execute("select DSESION from ut_st_sesion where IDSESION=" + str(sesion))
                    descripcion = fetch_single(cursor)
                    cursor.execute("select sum(nficheros) from ut_st_storage where IDSESION=" + str(sesion))
                    nficheros = fetch_single(cursor)
                    cursor.execute("select sum(nsize_original) from ut_st_storage where IDSESION="+ str(sesion))
                    nsize_original = fetch_single(cursor)
                    cursor.execute("select sum(nsize) from ut_st_storage where IDSESION="+ str(sesion))
                    nsize = fetch_single(cursor)
                    cursor.execute("select count(*) from ut_st_storage where IDSESION="+ str(sesion))
                    ntars = fetch_single(cursor)
                    cursor.execute("select count(*) from ut_st_sesion where IDSESION="+ str(sesion))
                    nsesiones = fetch_single(cursor)
                    cursor.execute("select count(distinct(ccuenta)) from ut_st_storage where IDSESION="+ str(sesion))
                    narchivados = fetch_single(cursor)
                    cursor.execute(
                        "select nficheros,ccuenta from ut_st_storage where nficheros = (select max(nficheros) from ut_st_storage where IDSESION="+ str(sesion)+") AND IDSESION="+ str(sesion))
                    maxficheros, cuenta_maxficheros = fetch_single(cursor)
                    cursor.execute("select nsize_original,ccuenta from ut_st_storage where nsize_original = \
                                   (select max(nsize_original) from ut_st_storage where IDSESION="+ str(sesion)+")")
                    maxsize_orig, cuenta_maxsize_orig = fetch_single(cursor)
                except BaseException, error:
                    print "Error recuperando información de la sesión ",sesion
                    print "ERROR: ", error
                    return

                compresion = 100-((nsize*100)/float(nsize_original))
                espaciobyusuario = nsize_original/narchivados

                print "\n"
                print "Sesión:\t\t", sesion
                print "Descripción\t", descripcion
                print "Archivados:\t", narchivados
                print "Numero tars:\t", ntars
                print "Ficheros:\t", nficheros
                print "Tamaño Orig:\t", size_to_human(nsize_original)
                print "Tamaño Arch:\t", size_to_human(nsize)
                print "Tasa Compresion\t",'%.2f' % compresion,"%"
                print "Tamaño/user\t",size_to_human(espaciobyusuario)
                print "Max ficheros:\t", maxficheros, "(", cuenta_maxficheros, ")"
                print "Max tamaño:\t", size_to_human(maxsize_orig), "(", cuenta_maxsize_orig, ")"
                print "\n"
            cursor.close()

    @staticmethod
    def do_sesiones(line):
        """Muestra todas las sesiones de archivado y caracteristicas
           sesiones"""
        from texttable import Texttable
        sesiones = []
        _ = line
        check_environment()

        try:
            cursor = config.oracleCon.cursor()
            cursor.execute("SELECT ses.IDSESION,ses.DSESION,st.NUSERS, st.TAM,st.NFICH \
                            FROM ut_st_sesion ses \
                            LEFT JOIN ( \
                                SELECT IDSESION,SUM(nsize_original) TAM, SUM(nficheros) NFICH, COUNT(DISTINCT(ccuenta)) NUSERS \
                                FROM ut_st_storage \
                                GROUP BY idsesion ) st \
                            ON ses.IDSESION = st.IDSESION \
                            ORDER BY ses.IDSESION"\
                            )
            sesiones = cursor.fetchall()
            cursor.close()
        except BaseException, error:
            print "Error recuperando todas las sesiones"
            print "ERROR: ", error
            cursor.close()
            return

        table = Texttable()
        table.add_row(["Id" , "Desc", "Cuentas", "Tamaño","Ficheros"])

        for sesion in sesiones:
            #print sesion[0]," ",sesion[1]
            table.add_row([sesion[0],sesion[1],sesion[2],size_to_human(sesion[3]),sesion[4]])
        print table.draw()

    def do_arcinfo(self,line):
        """
        Muestra información de archivado del usuario.

        arcinfo -s -i <usuario> [sesion]

        Si no se especifica -s y la sesión se muestran todas las sesiones.
        La opción -i muestra solo las sesiones en las que tiene archivados.
        """
        from texttable import Texttable

        args,items = self.parse_with_args(line,['-s','-i'],LIST)

        if not items:
            print "Se esperaba al menos un argumento"
            return False
        user = items[0]

        if '-i' in args:
            rows = get_sessions_by_user(user)
            table = Texttable()
            table.add_row(["IDSESION", "DSESION"])
            for row in rows:
                table.add_row([row[0], row[1]])
            print table.draw()
            return


        try:
            sesion = items[1]
            trailquery = " AND idsesion = " + sesion
        except:
            sesion = None
            trailquery = ""

        check_environment()

        cursor = config.oracleCon.cursor()
        query = "select * from ut_st_storage where ccuenta = " + comillas(user) + trailquery
        cursor.execute(query)
        rows = cursor.fetchall()

        if not '-s' in args:
            if not rows:
                print "Usuario no archivado"
                return False
            else:
                table = Texttable()
                table.add_row(["TARNAME", "SESION", "SIZE", "ORIGSIZE", "FILES"])
                for row in rows:
                    table.add_row([os.path.basename(row[2]), row[0], size_to_human(row[3]), size_to_human(row[5]), row[6]])
                print table.draw()
        else:
            return rows

    def do_sql(self,line):
        """Permite ejecutar una consulta sql directamente contra sigu
        sql [-l] <consulta>

        La opcion -l muestra una tabla sin limite de anchura, en caso
        contrario la anchura es la del terminal"""
        from texttable import Texttable

        check_environment()

        args,line = self.parse_with_args(line,['-l'],STRING)
        try:
            if config.DEBUG:
                debug("DEBUG-INFO: (do_sql) line: ",line)
            cursor = config.oracleCon.cursor()
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
            if '-l' not in args:
                trows,tcols = os.popen('stty size','r').read().split()
                table = Texttable(int(tcols)-1)
            else:
                table = Texttable(0)

            table.header(col_names)
            # table.set_deco(Texttable.BORDER | Texttable.HLINES | Texttable.VLINES)
            table.add_rows(rows, header=False)
            print table.draw()

    @staticmethod
    def do_trace(line):
        """Muestra o cambia el modo trace

           trace [True|False]
        """

        if line == "":
            print config.TRACE
        else:
            try:
                config.TRACE = ast.literal_eval(line)
                print config.TRACE
            except BaseException:
                print "Valor booleano incorrecto"

    @staticmethod
    def do_debug(line):
        """Muestra o cambia el modo debug

           debug [True|False]
        """

        if line == "":
            print config.DEBUG
        else:
            try:
                config.DEBUG = ast.literal_eval(line)
                print config.DEBUG
            except BaseException:
                print "Valor booleano incorrecto"

    @staticmethod
    def do_verbose(line):
        """Muestra o cambia el modo verbose

           verbose [nivel]
        """

        if line == "":
            print config.VERBOSE
        else:
            try:
                config.VERBOSE = ast.literal_eval(line)
                print config.VERBOSE
            except BaseException:
                print "Valor incorrecto"

    @staticmethod
    def do_ignorearchived(line):
        """Muestra o cambia si debe ignorar los usuarios ya archivados en la selección
            ignorearchived <True/False>"""

        if line == "":
            print config.IGNOREARCHIVED
        else:
            try:
                config.IGNOREARCHIVED = ast.literal_eval(line)
                print config.IGNOREARCHIVED
            except BaseException:
                print "Valor booleano incorrecto"

    @staticmethod
    def do_version(line):
        """Muestra la versión del programa"""
        print config.__version__

    def do_cuenta(self,line):
        """
        Muestra los datos de sigu de la cuenta

        cuenta <usuario>
        """

        self.do_sql("select * from ut_cuentas where ccuenta='"+line+"'")

    @staticmethod
    def do_checkaltdir(line):
        """Chequea y ofrece estadisticas de directorios alt para un directorio raiz dado
        checkaltdir <directorio>"""
        from collections import defaultdict

        _f = None
        ex_dirs = ('0_ALTHOME','0_UNITYMAIL')
        check_environment()
        dictdir = defaultdict(list)
        sumuserlist = 0
        parentdir = line + "/"
        altdirs = [s for s in os.listdir(parentdir)
                   if s.startswith(config.ALTROOTPREFIX) and not s.startswith(ex_dirs) ]

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
            if is_archived(k) == True:
                _f.write("\t" + "_ARC_")
            else:
                _f.write("\t" + "NOARC")

            for value in v:
                _f.write("\t" + value)
            _f.write("\n")
        fm.close()
        fs.close()
        print "LENDICT: ", len(dictdir)
        print "SUMLIST: ", sumuserlist

    @staticmethod
    def do_checkaltusers(line):
        """Chequea y ofrece estadisticas de usuarios con carpetas en directorios alt para un directorio raiz dado

        checkaltusers [-f tofile ] [-x directorio] <directorio>
        """

        fnosigunoldap = open("/tmp/ckaltusers_nosigunoldap","w")
        fnosigusildap = open("/tmp/ckaltusers_nosigusildap","w")
        fnoldap = open("/tmp/ckaltusers_noldap","w")
        farchivados = open("/tmp/ckaltusers_archivados","w")
        fexpirados = open("/tmp/ckaltusers_expirados","w")

        archived = 0
        expired = 0
        nosigunoldap = 0
        nosigusildap = 0
        noldap = 0

        ex_dirs = '0_althome'
        alluserlist = []
        _f = None
        check_environment()
        sumuserlist = 0
        parentdir = line + "/"
        altdirs = [s for s in os.listdir(parentdir)
                   if s.startswith(config.ALTROOTPREFIX) and not s.startswith(ex_dirs) ]

        for altdir in altdirs:
            #recupero la lista de usuarios
            userlist = os.listdir(parentdir + altdir)
            print "ALTDIR: ",altdir," LALL: ",len(alluserlist)," LUSR: ",len(userlist)
            alluserlist = list(set(alluserlist) | set(userlist))

        print "LEN ALLUSERLIST: ", len(alluserlist)

        for user in alluserlist:
            #Tiene cuenta en sigu o no?
            if not has_cuenta_sigu(user):
                if has_cuenta_ldap(user):
                    nosigusildap = nosigusildap + 1
                    print "NOSIGUSILDAP: ",user
                    fnosigusildap.write(user+"\n")
                    continue
                else:
                    nosigunoldap = nosigunoldap + 1
                    fnosigunoldap.write(user+"\n")
                    print "NOSIGUNOLDAP: ",user
                    continue
            #Tiene cuenta en ldap o no?
            if not has_cuenta_ldap(user):
                noldap = noldap + 1
                fnoldap.write(user+"\n")
                print "NOLDAP: ",user
                continue
            #Fue archivado previamente?
            if is_archived(user) == True:
                archived = archived + 1
                farchivados.write(user+"\n")
                print "ARCHIVED: ", user
                continue
            #Está expirado pero no se ha archivado?
            if is_expired(user):
                expired = expired + 1
                fexpirados.write(user+"\n")
                print "EXPIRED: ",user


        print "TOTAL: ",len(alluserlist)," NOSIGUNOLDAP: ",nosigunoldap,"NOSIGUSILDAP: ",nosigusildap," NOLDAP: ",noldap," ARCHIVADOS: ",archived," EXPIRADOS: ",expired

        fnosigunoldap.close()
        fnosigusildap.close()
        fnoldap.close()
        farchivados.close()
        fexpirados.close()

    @staticmethod
    def do_quit(line):
        print "Hasta luego Lucas ...."
        _ = line
        os._exit(True)

    def __init__(self):
        cmd.Cmd.__init__(self)

    def do_fromfile(self,line):
        """
        Selecciona el fichero fromfile para lectura de lista de Usuarios

        fromfile [-d] <fichero>

        Sin argumentos muestra si está definido FROMFILE.
        La opción -d quita la definición que hubiera.
        """

        args,line = self.parse_with_args(line,['-d'],STRING)

        if '-d' in args:
            config.FROMFILE = None
            print "Quitado fichero fromfile"
            return

        if not line:
            if config.FROMFILE:
                print config.FROMFILE
            else:
                print "Fromfile no estaba definido"
            return

        if os.path.exists(line):
            config.FROMFILE = line
        else:
            print "No existe el fichero ",line

    def do_tofile(self,line):
        """
        Selecciona el fichero de salida para los comandos que generan una lista de Usuarios

        tofile fichero
        """
        if not line:
            if config.TOFILE:
                print config.TOFILE,
                if config.TOFILEHANDLE:
                    print " está abierto"
                else:
                    print "no está abierto"
                return
            else:
                print "No se ha especificado TOFILE"
                return

        if not os.path.exists(line):
            config.TOFILE = line
            open_tofile()
        else:
            print "Ya existe el fichero ",line

    def do_historia(self,line):
        """
        Muestra la historia de sigu de un usuario.

        historia <usuario>
        """

        self.do_sql("select * from UT_HIST_CTAS where ccuenta='"+line+"'")

    def do_servicesoff(self,line):
        """
        Muestra si un usuario tiene todos los servicios a off.

        servicesoff <usuario>

        Si el usuario no existe en LDAP devuelve True.
        """

        config.WINDOWS_PASS = "dummy"
        check_environment()

        print all_services_off(line)

    def do_unarchive(self,line):
        """
        Desarchiva un usuario de una sesión determinada.

        unarchive -f -b <usuario> <sesion>

        La opción -f fuerza el desarchivado si ya existía el storage correspondiente.
        La opción -b borra también los registros de la BBDD.
        Si no se especifica la sesión se desarchiva la última.
        """

        config.WINDOWS_PASS = "dummy"
        check_environment()
        force = False
        delbbdd = False
        test = False

        args,items = self.parse_with_args(line,['-f','-b'],LIST)

        if '-f' in args:
            force = True
        if '-b' in args:
            delbbdd = True

        if len(items)<2:
            print "Se esperaban dos argumentos"
            return False
        user = items[0]
        sesion = items[1]

        #Comprobaciones previas
        rows = self.do_arcinfo(user + " " + sesion + " -s")

        if not rows:
            print "No hay nada que desarchivar"
            return

        #Mount points
        if config.MOUNT_EXCLUDE == "(?=a)b":
            config.MOUNT_EXCLUDE = "/nfs/"
        check_mounts()

        #Procesamos todos los archivados
        for row in rows:
            arcitems = parse_arctar(row[2])
            if not arcitems:
                #Era un dummy, procesamos el -b aquí
                if delbbdd:
                    delete_bbdd_storage(row[0],row[1],row[2])
                continue
            tarfile = row[2]

            #averiguamos el destdir
            if arcitems[3]:
                traildir = "/" + arcitems[3] + "/" + row[1] + "/"
            else:
                traildir = "/" + row[1] + "/"
            #Ahora tenemos que averiguar el basedir
            for m in config.MOUNTS:
                if m['fs'] == arcitems[2]:
                    basedir = m['val']
                    found = True
                    break
                else:
                    found = False

            if not found:
                print "Fallo al encontrar el basedir de ",arcitems[2]
                return

            if test:
                basedir = "/tmp/unarchive/" + arcitems[2]
            destdir = basedir + traildir
            if config.DEBUG:
                debug("SOURCE: ",tarfile,"DESTDIR: ",destdir)

            #desarchivamos
            unarchived = unarchive_tar(tarfile,destdir,force)

            #Borramos de la BBDD
            if delbbdd and unarchived:
                delete_bbdd_storage(row[0],row[1],row[2])
