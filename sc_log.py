#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
"""
Created on Mon May 20 09:09:42 2013

@author: tonin
"""
import os

import config
from sc_funcs import *

class Log(object):
    """Clase que proporciona acceso a los logs"""

    def __init__(self, session):
        self.session = session
        # Creamos el directorio logs si no existe. Si existe renombramos el anterior
        if config.CONSOLIDATE:
            session.logsdir = session.tardir + "/consolidatelogs"
        else:
            session.logsdir = session.tardir + "/logs"
        if not os.path.exists(session.logsdir):
            os.mkdir(session.logsdir, 0777)
        else:
            # Tenemos que tener en cuenta de si es una sesion restore
            # caso de no serla rotamos el log.
            # si  lo es, usamos el mismo log solo para cmdline ya que el fork lo rotara
            if not config.RESTORE:
                new_name = unique_name(session.logsdir)
                os.rename(session.logsdir, new_name)
                os.mkdir(session.logsdir, 0777)
        # Si es restore salimos sin crear fichero ninguno
        if config.RESTORE:
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
        config.fAllOutput = self.fAllOutput
        self.fCreateDone = open(session.logsdir + '/create.done', "w")
        self.fRenameDone = open(session.logsdir + '/rename.done', "w")
        self.fRenameFailed = open(session.logsdir + '/rename.failed', "w")
        self.fManualDelete = open(session.logsdir + '/storages.manualdelete',"w")

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

    def write_manualdelete(self, string):
        self.fManualDelete.writelines(string + "\n")
        self.fManualDelete.flush()
        self.session.stats.manualdelete += 1

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
        if not config.RESTORE:
            trail = "\n" if newline else ""
            self.fLogfile.write(string + trail)
            self.fLogfile.flush()

    def write_all_output(self, string, newline):
        # En sesiones restore no escribimos el log
        if not config.RESTORE:
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
