SiguClean
=========

SiguClean es una utilidad para archivar los espacios de almacenamiento asociados a una cuenta de usuario de sigu. 

Conceptos Clave
---------------

* **storage:**
    Un determinado almacenamiento. Por ejemplo, el home de correo, home de nfs, etc.

* **usuario:**
    Un usuario de sigu que tiene asociados uno o varios storages

* **sesion:**
    Una ejecución de siguclean que procesará una lista de usuarios. Una sesión se identifica por un nombre que se tomará como nombre de la carpeta dentro de `sesiondir`

* **mandatory:**
    Atributo de un storage que ocasiona que en caso de fallar su archivado, se considere que el archivado global del usuario ha fallado.

* **rollback:** 
    Proceso que deshace los cambios realizados durante el archivado de un usuario. Su objetivo es dejar todo lo relativo a dicho usuario como si no hubiera sido procesado en dicha sesión.

* **sesiondir:**
    Directorio donde se almacenarán todos los usuarios archivados. La estructura contendrá una carpeta por cada usuario archivado y una carpeta logs con los ficheros de salida. La carpeta de usuario contendrá tantos tar.bz como storages se archiven aparte de un fichero con un volcado de la DN del usuario de active directory.

Operativa
---------
SiguClean puede funcionar de forma interactiva ofreciendo una shell con unos pocos comandos de utilidad, o bien mediante el paso como opciones del comando `siguclean.py` de todos los parámetros de ejecución.

La base de siguclean es que sea robusto. Para ello sigue varias máximas:

* Las operaciones con usuarios son transaccionales. Si fallan los archivados mandatory u operaciones que no deben fallar, se realiza un rollback sobre el mismo para que quede en disposición de ser archivado más adelante cuando se corrija el problema.
* Las condiciones de abortado del programa son muy estrictas, abortando solo cuando el problema impide procesar ningún usuario, o cuando los fallos se van acumulando y superan cierto límite. 
* Se deja información categorizada del problema que ha hecho que cada usuario falle. Aparte activando la opción de debug o niveles de verbose más altos, se dispone de suficiente información para depurar el proceso.
* La mayoría de los bloques de código donde se pueden presentar errores inesperados, se encuentran en bloques try-except para interceptar dichos errores.

### Opciones referentes a la operativa ###

**--help**: Presenta la ayuda de los parámetros

**-i**: Ejecuta en modo interactivo.

**-a, --abortalways**: Aborta el programa siempre que se produzca un error inesperado (default: False)

**-l, --abortlimit** *numero errores*: Límite de errores para que el programa aborte. 0 para no abortar nunca (default: 5)

**-d, --abortdecrease**: La cuenta de errores se decrementa cada vez que se produzca un archivado correcto. Esto sirve para aislar problemas puntuales que se puedan producir por problemas de nfs, ldap, etc. (default: False)

**-s, --abortinseverity**: Abortar si se produce un error de tipo severo. El resto de errores se procesarán según los parámetros anteriores. La severidad o no de un error esta "hardcoded" en el código. (default: False)

**-m, --maxsize** *tamaño máximo*: Límite de tamaño de archivados que una vez alcanzado ocasiona que el resto de usuario a procesar se salten (sean "skipped"). Su objetivo es no sobrepasar el espacio asignado para el directorio donde se archivarán los storages de los usuarios. Si vale 0 no se aplica límite ninguno. Si vale àuto`se calcula el máximo en función del tamaño máximo del filesystem destino para los tars, con un margen de seguridad del 10% (Default: 0)

**--debug**: Genera información de depuración, tanto por pantalla como en el fichero debug. (Default: False)

**-v**: Nivel de detalle de los mensajes. Se incrementa poniendo repetidas "v", por ejemplo -vvv sería un nivel de verbose 3. (Default: 0)

**--progress**: Muestra una barra de % de progreso y de ETA estimado. Solo se muestra si tenemos verbose a 0 y si debug no está activado. (Default: False)

**--confirm**: Pide confirmación para continuar una vez que ha recopilado la información sobre los usuarios que va a procesar. (Default: False)

Opciones de sesión
------------------

**--sessiondir** *directorio*: Directorio raiz para almacenar las sesiones de archivado. Normalmente usaremos un montaje exclusivo para ello. Tendremos en cuenta antes de arrancar la sesión de archivado comprobar que el espacio libre es suficiente, especificando en su caso con el parámetro *maxsize* un límite para que no se llene.

**-n** *SESSIONID*: Nombre de la sesión de archivado. Todos los archivados se encontrarán en una carpeta con este nombre debajo de *sessiondir*. Se recomienda usar nombres descriptivos sobre el fin de la sesión, fecha o combinaciones de ella.

Opciones de control de cambios
------------------

Siguclean en condiciones normales realiza escrituras en diversos lugares: Storages de usuario, tars de los storages, BBDD de sigu y directorio activo. Podemos controlar si escribirá o no con las siguientes opciones:

**--dry-run**: No realiza ninguna operación de escritura sobre ubicaciones originales. Si creará los tars y las carpetas que los contienen pero con tamaño 0. (defaul: False)

**--soft-run**: Usada junto a la anterior, si genera los tars e inserta registros en la BBDD de sigu, pero no toca los storages originales ni AD. Es útil para probar más a fondo que el proceso se desarrollará sin problemas, sin realizar operaciones de escritura que no sean fáciles de deshacer.

Opciones de selección
---------------------

En siguclean podemos realizar varias selecciones que afectan al proceso del programa, están controladas por las siguientes opciones:

### Selección de usuarios ###

**-t, --to** < fecha >: Fecha hasta la que se seleccionan usuarios, en el formato `YYYY-MM-DD`. (Default: None)

**-f, --fromdate** < fecha >: Fecha desde la que se seleccionan usuarios en el mismo formato que la anterior. Si no se especifica se asume una fecha muy en el pasado para que al final se seleccione cualquier usuario afectado hasta la fecha anterior. (Default: None)

**--fromfile** < fichero >: En lugar de consultar los usuarios en sigu, se usan los del fichero especificado donde cada usuario va en una línea distinta. Es útil cuando se quiere volver a lanzar el procedimiento solo con los usuarios de cualquiera de los ficheros resultado que contenga usuarios que han fallado.

**--ignore-archived**: Excluye directamente de la selección de sigu o del fichero de usuarios aquellos que ya están archivados para que no aparezcan como excluidos. Muy útil si se lanza un archivado para un rango de fechas que previamente fue procesado con el objeto de procesar solo los que tuvieron problemas y ya se han corregido.

### Selección de orígenes de storages ###

Los orígenes de storages son los lugares del sistema de ficheros en los que esperamos encontrarnos los storages de los usuarios. En principio existe una lista de diccionarios hardcoded donde se especifican los diferentes filesystem que se usarán. Actualmente la tabla es esta:

`    MOUNTS = ({'account':'LINUX','fs':'homenfs','label':'HOMESNFS','mandatory':True,'val':''},
              {'account':'MAIL','fs':'homemail','label':'MAIL','mandatory':True,'val':''},  
              {'account':'WINDOWS','fs':'perfiles','label':'PERFILES','mandatory':False,'val':''},  
              {'account':'WINDOWS','fs':'homecifs','label':'HOMESCIF','mandatory':True,'val':''})`
              
Los campos de la misma son:

* **account**:  Tipo de cuenta para el que tiene sentido dicha entrada. Si el usuario tiene cuenta de ese tipo, se usará, si no, no.
*  **fs**: Nombre interno que manejará el programa como clave o identificativo para dicho filesystem.
*  **label**: Estiqueta que se buscará en `/proc/mounts` para localizar donde se ha montado dicho filesystem.
*  **mandatory**: Si el filesystem es mandatory o no.
*  **val**: Aquí siguclean almacenará el pathname concreto que resuelva una vez que consulte los labels de los montajes hechos.

En base a esto, podemos tener más control sobre los orígenes de storages con  
las siguientes opciones:

**-x** *MOUNTEXCLUDE*: Especificamos una expresión regular que excluirá aquellos montajes devueltos en val que la cumplan. Por ejemplo, si tenemos el mismo filesystem (con el mismo label por tanto) montado en dos lugares diferentes (/nfs y /nfsro), podemos excluir el primero usando "-x /nfs/" o el segundo con "-x /nfsro/" (Default: None)

**-p** *ALTROOTPREFIX*: En principio siguclean buscará storages de usuarios buscando carpetas con el nombre de usuario de los distintos val que se devuelvan, en función de que el usuario disponga de cuenta de ese tipo o no. Si la carpeta es un enlace simbólico a otro sitio, siguclean resolverá el enlace. Pero se puede dar el caso de que los storages se hubieran movido a otros sitios para hacer espacio. En este caso deberiamos crear un enlace simbólico en la raiz del montaje que apuntara a dicha ubicación con el nombre que queramos pero comenzando por *ALTROOTPREFIX*. Por ejemplo, si hemos movido usuarios a una carpeta que tenemos montada en /nfs/MOVIDOS2013 para los homes de correo, y estos están en /nfs/MAIL, creariamos aquí un enlace a /nfs/MOVIDOS2013 con el nombre 0_MOVIDOS2013. En este caso *ALTROOTPREFIX* es "0_" y deberemos usarlo para todos los enlaces que hagamos. (Default: None)

Como resulta costoso en tiempo generar la lista de ubicaciones alternativas si lo hacemos para cada usuario, esta se crea cuando se procesa el primer usuario que la necesita y para el resto de usuarios ya se encuentra resuelta y construida.

**-w,--windows-check** *{ad,sigu,both}*: Para comprobar si un usuario tiene cuenta de windows, podemos consultar la tabla correspondiente de sigu o el directorio activo. No existe una correspondencia uno a uno entre ambas. Con esta opción especificamos donde hacer la consulta. Si seleccionamos *both*, se considerará que el usuario tiene cuenta de windows si se resuelve por ambas consultas simultaneamente. (Default: sigu)

Opciones de autenticación
-------------------------

**--win-password** *password*: Clave del administrador de windows en el dominio uco.es

**--sigu-password** *password*: Clave del usuario sigu.

En caso de no haber especificado dichas claves siguclean las pedirá por consola cuando realize las primeras comprobaciones.

Ficheros de salida
------------------

Siguclean genera una carpeta por cada usuario con su nombre con los archivados del mismo. El nombre de cada archivado es de la siguiente forma:

*usuario_filesystemkey_sessiondir*.tar.bz2

Los ficheros de log se encuentran en la carpeta logs en al raiz de *sessionid* y son:

* **logfile**: Contiene toda la salida por pantalla para todos los niveles de verbose independientemente del nivel de verbose seleccionado. Es el equivalente a haber seleccionado un verbose muy alto y redirigir la salida a un fichero.
* **debug**: Contiene toda la salida de la información de depuración.
* **bbddlog**: Contiene todas las secuencias insert de sql que se han generado para sigu. Nos permite en caso de que haya fallado la inserción poder reproducirla posteriormente.
* **users.list**: Contiene todos los usuarios que se procesarán en la sesión en base a los criterios de selección.
* **users.done**: Usuarios archivados correctamente.
* **users.failed**: Usuarios en los que ha fallado el archivado.
* **users.rollback**: Usuarios que habiendo fallado ha habido que hacerles rollback.
* **users.norollback**: Usuarios que habiendo tenido que hacerles rollback, este ha fallado inesperadamente.
* **users.skipped**: Usuarios que no se han procesado por haberse llegado al limite de *maxsize*
* **users.excluded**: Usuarios que se excluyen del archivado porque se ha producido una circunstancia que causa la exclusión. Las circunstancias pueden ser:
      - El usuario tiene estado de archivado, esto es, existen archivados de él posteriores a la fecha en la que caducó o se canceló.
      - El estado de archivado del usuario no se ha podido recuperar de *sigu*
      - El usuario no tiene *homedirectory* en *ldap*. Lo más normal es que o no tenga entrada en *ldap* o la tenga en una rama distinta de *people*
* **failreason**: Este fichero contiene un listado de todos los usuarios y la razón por la que han fallado o han sido excluidos. El formato es:

                *usuario*    *reason*    *info*

Las razones pueden ser:

#### Razones de fallo ####

- *NOMANDATORY*: Falta un storage mandatory para el usuario. En el campo info se dice cual.
- *FAILARCHIVE*: Ha habido un problema en la creación o escritura de los tar.
- *FAILDELETE*: Ha habido un problema al borrar el origen del storage tras el archivado.
- *FAILARCHIVEDN*: Ha habido un problema al archivar la DN del usuario de active directory.
- *FAILDELETEDN*: Ha habido un problema al borrar la DN del usuario de active directory.
- *UNKNOWN*: Se ha producido un fallo desconocido en el proceso de archivado.

#### Razones de exclusión ####

- *NOTINLDAP*: El usuario no tiene entrada en la rama *People* de ldap.
- *ISARCHIVED*: El usuario ha sido excluido por estar ya archivado.
- *UNKNOWNARCHIVED*: No se ha podido recuperar el estado de archivado o no de sigu.
- *NODNINAD*: EL usuario no tiene DN en active directory teniendo que tenerla al tener cuenta windows. Lógicamente solo se producirá este error si la consulta de cuenta windows se hace solo por sigu y no por ad o both.


Modo interactivo
----------------

En el modo interactivo contamos con una serie de comandos al estilo de "navaja suiza" que nos pueden ayudar para planificar nuestra ejecución o para consultar y depurar problemas sin tener que recurrir a otras herramientas. Los comandos son:

**help** *comando*: Sin argumento muestra los comandos disponibles, con argumento la ayuda de un comando específico.

**count** *fromdate* *todate*: Nos dice cuantos usuarios se encuentran en el estado caducado o cancelado entre ambas fechas.

**hascuentant** *usuario*: Nos dice si un usuario tiene o no cuenta nt. El método empleado para la consulta es el del parámetro -w.

**isarchived** *usuario*: Nos dice si un usuario tiene el status de archivado. Si preguntamos por un usuario activo devolverá True, no porque esté archivado, sino porque no tiene que archivarse. De esta forma actua como medida de seguridad si por error en la selección a partir de fichero metemos usuarios no caducados ni cancelados.

**ldapquery** *usuario* *atributo*: Nos devuelve el valor del atributo ldap para dicho usuario.

**quit**: Sale del programa

 
