#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
# from __future__ import print_function



"""
from shutil import rmtree
import tarfile
from pprint import pprint
import pickle
#import re
import collections

from enum import Enum
import dateutil.parser
from progressbar import *

import readline
"""
import sys
import os
import subprocess

import config
from sc_shell import Shell
from sc_funcs import *
from sc_funcs import _print
from sc_classes import *

"""
Programa principal
"""
import argparse
parser = argparse.ArgumentParser(description='Siguclean 1.0.1: Utilidad para borrar storages de usuarios',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-n', '--sessionname', help='Nombre de sesion', dest='sessionId', action='store', default=None)
parser.add_argument('-p', '--altrootprefix', help='Prefijo para carpetas alternativas', dest='ALTROOTPREFIX',
                    action='store', default=None)
parser.add_argument('-i', '--interactive', help='Iniciar sesion interactiva', action='store_true')
parser.add_argument('--version', help='Mostrar la versión del programa', action='store_true')
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
parser.add_argument('--extra-debug', help='Imprimir mensajes extras de depuración', dest='EXTRADEBUG', action='store_true')
parser.add_argument('--count-debug', help='Imprimir mensajes extras de depuración', dest='COUNTDEBUG', action='store_true')
parser.add_argument('--dry-run', help='No realiza ninguna operación destructiva sobre los orígenes ni toca la BBDD', dest='DRYRUN', action='store_true')
parser.add_argument('--soft-run', help='Junto a dry-run, si genera los tars y la inserción en la BBDD', dest='SOFTRUN',
                    action='store_true')
parser.add_argument('--dry-no-write', help='No realiza ninguna operación de escritura de tars en dry-run', dest='DRYNOWRITE', action='store_true')
parser.add_argument('--count-run', help='Calcula solamente el espacio ocupado por los storages de los usuarios', dest='COUNTRUN', action='store_true')
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
                    action='store', default=config.Mrelax.NONE)
parser.add_argument('--ldap-relax', help='No tiene en cuenta si el usuario está o no en ldap', dest='LDAPRELAX',
                    action='store_true')
parser.add_argument('--check-archived-data', help='Chequea que el usuario efectivamente tiene archivados aunque sigu diga que está archivado', dest='CHECKARCHIVEDDATA',
                    action='store_true')
parser.add_argument('--delete-relax', help='No falla el usuario si no es capaz de borrar determinado storage, en su lugar pone el path en storages.manualdelete para borrarlos mas tarde', dest='MANUALDELETE',
                    action='store_true')
parser.add_argument('--trace', help='Imprime información de trazado', dest='TRACE',action='store_true')
parser.add_argument('--consolidate-only-fs',help='Solo consolida los fs especificados',dest='CONSOLIDATELIST',nargs='+', choices=('homenfs', 'homecif', 'homemail', 'perfiles'))
parser.add_argument('--consolidate-pass1',help='Ejecuta el pass1 y pass2 de consolidación, si no solo el pass2',dest='CONSOLIDATEPASS1',action='store_true')
args = parser.parse_args()

_ = config.VERBOSE
config.VERBOSE = args.verbosity
# NOTA: Los debugs previos a la creación de la sesión no se pueden almacenar en el fichero
# debug pues aun no hemos establecido la rotación de logs

#Leemos primero si está la opción debug

if args.DEBUG:
    config.DEBUG = True
if args.RESTORE:
    config.RESTORE = True


if config.DEBUG and not config.RESTORE:
    debug('verbose es: ', config.VERBOSE)

# Si no es interactiva ponemos los valores a config

for var in args.__dict__:
    if var in config.__dict__:
        if vars(args)[var] is not None:
            if config.DEBUG:
                debug('DEBUG-INFO: existe ', var, ' y es ', vars(args)[var])
            config.__dict__[var] = vars(args)[var]

#os._exit(True)
if args.interactive:
    Shell().cmdloop()
    os._exit(True)

if args.version:
    print __version__
    os._exit(True)

if config.COUNTRUN:
    debug("DEBUG-INFO: Forzamos DRYNOWRITE al tener COUNTRUN")
    config.DRYNOWRITE = True
    debug("DEBUG-INFO: Forzamos DRYRUN al tener COUNTRUN")
    config.DRYRUN = True

if config.SOFTRUN and not config.DRYRUN:
    debug("DEBUG-INFO: Forzamos DRYRUN al estar SOFTRUN")
    config.DRYRUN = True

if config.DRYNOWRITE and not config.SOFTRUN:
    debug("DEBUG-INFO: Forzamos DRYRUN al tener DRYNOWRITE")
    config.DRYRUN = True

if config.DEBUG and not config.RESTORE and not config.CONSOLIDATE:
    debug('DEBUG-INFO: SessionId: ', config.sessionId, ' Fromdate: ', config.fromDate,
          ' toDate: ', config.toDate, ' Abortalways: ', config.ABORTALWAYS, ' Verbose ',
          config.VERBOSE)

cmdline = sys.argv
cmdlinestr = ' '.join(cmdline)
sesion = None

try:
    sesion = Session(config.sessionId, config.fromDate, config.toDate,"MAIN")
except BaseException, e:
    _print(0, 'ABORT: Error en la creación de la sesion')
    _print(0, "ERROR: ", e)
    os._exit(False)
# Guardamos los argumentos
# Si no es una sesión restore salvamos el string
if not config.RESTORE:
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

if not config.CONSOLIDATE:
    sesion.start()
else:
    # Estamos en una sesion de consolidacion. Algunas comprobaciones previas
    _print(0, "... Consolidando sesion anterior ...")
    # Comprobamos que existe la sesion y leemos el id
    if not sesion.get_session_id():
        os._exit(False)
    if config.DEBUG:
        debug("DEBUG-INFO: Idsession es ", sesion.idsesion)
    # Generamos la lista de usuarios a partir de fromfile, o archivados al estar en consolidacion
    if not sesion.get_account_list():
        os._exit(False)
    if config.DEBUG:
        debug("DEBUG-INFO: Recuperada lista usuarios desde FS. Numero usuarios: ", len(sesion.accountList))

    #PASO 1 de CONSOLIDACION: Solo lo invocamos si expresamente lo seleccionamos --consolidate-pass1
    if args.CONSOLIDATEPASS1:
        if not args.CONSOLIDATELIST:
            sesion.consolidate_fs('homenfs')
            sesion.consolidate_fs('homemail')
            sesion.consolidate_fs('perfiles')
            sesion.consolidate_fs('perfilesv2')
            sesion.consolidate_fs('homecifs')
        else:
            sesion.consolidate(args.CONSOLIDATELIST)
    #PASO 2 de CONSOLIDACION
    sesion.start()
os._exit(True)
