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

* **Alternativo:**
    Refiriéndonos a un storage, le decimos alternativo si no se encuentra en la ubicación normal predefinida. A las ubicaciones que almacenan storages alternativos les denominamos ubicaciones alternativas. Los storages alternativos pueden venir de movimientos manuales de storages para aumentar el espacio, o de ubicaciones diferentes dependiendo del tipo de usuario, como pasa con los homescif de biblioteca y del resto de usuarios. No consideramos alternativa las diferentes ubicaciones de los homes de correo ya que todas son accesibles desde la principal mediante el correspondiente enlace.

* **Adicional:**
    Refiriéndonos a un storage de determinado tipo de un usuario, sería su storage principal el que se encuentra en la ubicación definida por defecto, y adicional el que se encuentra en ubicaciones alternativas. La diferencia con el concepto anterior es sutil. Si un usuario no tiene storage por ejemplo de correo en la ubicación por defecto y tiene uno en una alternativa, diriamos que tiene un storage alternativo y no tiene storage adicional. Sin embargo si tiene un storage en la ubicación por defecto y otro en una ubicación adicional diriamos que este segundo storage es adicional y no alternativo.

Operativa
---------
SiguClean puede funcionar de forma interactiva ofreciendo una shell con unos pocos comandos de utilidad, o bien mediante el paso como opciones del comando `siguclean.py` de todos los parámetros de ejecución.

La base de siguclean es que sea robusto. Para ello sigue varias máximas:

* Las operaciones con usuarios son transaccionales. Si fallan los archivados mandatory u operaciones que no deben fallar, se realiza un rollback sobre el mismo para que quede en disposición de ser archivado más adelante cuando se corrija el problema.
* Las condiciones de abortado del programa son muy estrictas, abortando solo cuando el problema impide procesar ningún usuario, o cuando los fallos se van acumulando y superan cierto límite. 
* Se deja información categorizada del problema que ha hecho que cada usuario falle. Aparte activando la opción de debug o niveles de verbose más altos, se dispone de suficiente información para depurar el proceso.
* La mayoría de los bloques de código donde se pueden presentar errores inesperados, se encuentran en bloques try-except para interceptar dichos errores.

### Interrupción del procesamiento forzada por el usuario###

Mediante la creación de un fichero llamado *STOP* en la raiz del directorio de sesión, obligamos a siguclean a detenerse al terminar el procesamiento del usuario actual antes de comenzar con el siguiente. Es preferible usar este método a parar el script directamente para asegurarnos de que no se queda ningún usuario a medio archivar.

Como es lógico, usaremos este mecanismo en caso de que estemos haciendo un pre-chequeo con --dry-run y no queramos completarlo, o bien si detectamos en la salida por pantalla o en los ficheros de log que algo está funcionando realmente mal y no merece la pena continuar.

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

### Opciones de chequeo ###

Siguclean es bastante estricto para decidir con carácter previo si un usuario puede ser archivado o no. Se puede relajar el chequeo previo del usuario con las siguientes opciones:

**--mandatory-relax** *nivel de relajación*: Relaja la comprobación de que los storages mandatory existen y están accesibles. El nivel puede ser:

- 1 No hay relajación, se comporta igual que si no usamos la opción.
- 2 No se comprueba la accesibilidad de los mandatory si la cuenta está CANCELADA.
- 3 No se comprueba la accesibilidad de los mandatory en ningún caso.

**--ldap-relax**: No se comprueba la existencia del usuario en ldap archivándolo aunque no exista. El problema colateral en este caso es que al no poder consultar el homedirectory de correo en ldap tenemos que asumir que se encuentra en la ubicación por defecto que está hard-coded en el programa.

Opciones de sesión
------------------

**--sessiondir** *directorio*: Directorio raiz para almacenar las sesiones de archivado. Normalmente usaremos un montaje exclusivo para ello. Tendremos en cuenta antes de arrancar la sesión de archivado comprobar que el espacio libre es suficiente, especificando en su caso con el parámetro *maxsize* un límite para que no se llene.

**-n** *SESSIONID*: Nombre de la sesión de archivado. Todos los archivados se encontrarán en una carpeta con este nombre debajo de *sessiondir*. Se recomienda usar nombres descriptivos sobre el fin de la sesión, fecha o combinaciones de ella.

**--restore**: Esta opción se debe usar solo con las dos opciones anteriores (o solo con *-n* si usamos el directorio por defecto). Restaura una sesión anterior previamente interrumpida, ya sea porque el programa abortó, porque nosotros lo matamos o porque el servidor se rebotó.

Su cometido es restaurar una sesión previamente ejecutada. Usará el mismo directorio de sesión y en la BBDD los registros se añadirán usando la misma clave de sesión restaurada. Las opciones de ejecución serán las mismas con las que se lanzó la sesión original.

Para ello en cada ejecución de una sesión se salva la línea de comandos en el fichero cmdline en el directorio de logs correspondiente *(ver la sección ficheros de log)*

Si miramos con `ps` los procesos siguclean en ejecución, veremos que tras un restore aparte del proceso con dicha opción, aparece otro hijo de él con la opción *--ignore-archived* activada (aunque no la hubiéramos seleccionado antes), y con otra opción *--restoring < n >* donde *n* es el id de la sesión en la BBDD. La opción *--restoring* es de uso interno y no debe ser invocada directamente por el usuario.

Al final del proceso, el global de usuarios procesados en la sesión para cada categoría de éxito, fallo, exclusión, etc. será la unión de los que aparezcan en los diferentes directorios de logs que se hayan ido creando para la sesión.

Se puede lanzar un restore de una sesión tantas veces como se quiera, incluso aunque la sesión hubiera finalizado correctamente. En este último caso, si todos los usuarios han sido procesados el programa saldrá inmediatamente. Si hubiera terminado pero con fallo en algunos, se volverán a procesar dichos fallos, muy útil si se ha corregido algo que pueda ampliar la posibilidad de éxito de la sesión.

**--consolidate**: Esta opción se introdujo con posterioridad al darnos cuenta de que muchos usuarios tenían multiples storages para el homemail, debido a movimientos manuales para hacer sitio. Una sesión de consolidación trabaja no sobre una selección sino sobre el conjunto de usuarios que ya han sido archivados. Por tanto es imprescindible disponer en línea del directorio de archivado de dicha sesión.

La consolidación realiza 2 pasos:

1. Se extrae de los logs la ubicación correspondiente al origen del storage. Si la ubicación es alternativa se renombran los ficheros de archivado correspondiente para que correspondan a ella. Por ejemplo, si el storage se archivó como *usuario@homemail@sesion* ... pero correspondia a la ubicación "MOVIDOS", se renombra como **usuario@homemail=MOVIDOS@sesion** ...
2. Se lanza una sesión sobre el conjunto de usuarios archivados buscando storages adicionales para archivar.

Los logs de consolidación van al directorio especial consolidatelogs que sigue el mismo esquema de rotación que el directorio logs por si fallara algo y hubiera que volver a lanzarlo.

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

**--exclude-userfile** < fichero >: Lo contrario de lo anterior. Especifica un fichero con un conjunto de usuarios que aún estando en la selección serán excluidos del procesamiento. 

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

Los ficheros de log se encuentran en principio en la carpeta logs en al raiz de *sessionid*. Existe un mecanismo de rotación de logs por si se lanzan ejecuciones sucesivas con el mismo identificador de sesión. El sistema de rotación ocasiona que:

* La última ejecución lanzada esté en la carpeta logs.
* Caso de haber más de una, la primera lanzada esté en logs.0. La segunda en logs.1 y así sucesivamente.

Los  ficheros presentes en la carpeta de logs son:

* **cmdline**: La línea de comando que se usó para lanzar el proceso. Las sesiones de restore no generan una carpeta de log como tal, la genera la sesión hija que lanza. Por tanto no debemos esperar encontar ningún `cmdline` con la opción *--restore*
* **idsesion**: Almacena el identificador de sesión usado en la BBDD, por tanto el valor numérico generado secuencialmente en ella.
* **users.current**: Almacena el usuario que se está procesando en un momento dado. Si el proceso termina correctamente, tendrá el último usuario que se procesó. Si el proceso falla o se corta inesperadamente, tendrá el usuario que comenzó a procesar y no se completó. Nos puede servir para analizar manualmente que pasa con los usuarios que se quedan a medio procesar y que lógicamente quedan fuera de la lógica del programa.
* **logfile**: Contiene toda la salida por pantalla para todos los niveles de verbose independientemente del nivel de verbose seleccionado. Es el equivalente a haber seleccionado un verbose muy alto y redirigir la salida a un fichero.
* **debug**: Contiene toda la salida de la información de depuración.
* **alloutput**: Contiene todo el contenido de logfile y debug al mismo tiempo. Sería el equivalente a la salida por consola de la ejecución.
* **bbddlog**: Contiene todas las secuencias insert de sql que se han generado para sigu. Nos permite en caso de que haya fallado la inserción poder reproducirla posteriormente.
* **rename.done**: En sesiones de consolidación, almacena aquellos ficheros que han sido renombrados al haber encontrado una ubicación alternativa.
* **rename.failed**: En sesiones de consolidación, almacena aquellos ficheros que han fallado al renombrarse.
* **create.done**: Almacena todos los ficheros de archivado que se han creado con su path completo. No tiene sentido almacenar los que han fallado porque un fallo en la creación de un fichero de archivado produce inmediatamente el rollback del usuario.
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

**advsarchived** *fromdate* *todate*: Nos devuelve un listado con aquellos usuarios archivados que aun tienen cuenta en active directory. Si no se especifican las fechas, se supone todo el rango temporal.

**schacquery** *usuario*: Consulta el atributo schacUserStatus de ldap de dicho usuario.

**allservicesoff** *usuario*: Comprueba si todos los servicios de un usuario están a off.

**archived** *fromdate* *todate*: Lista los usuarios que se han archivado entre dos fechas dadas (basados en la caducidad en ut_cuentas). En caso de no especificar las fechas se toma todo el rango temporal.

**isexpired** *usuario*: Muestra si un usuario está expirado (esto es, caducado o cancelado).

**stats**: Muestra estadísticas globales sobre el proceso de archivado. Su salida es como esta:
```
*********************************
*** ESTADISTICAS DE SIGUCLEAN ***
*********************************
Sesiones:    11
Archivados:	31843
Numero tars:	124364
Ficheros:	13132214
Tamaño Orig:	608.1 G
Tamaño Arch:	409.5 G
Max ficheros:	98470 ( i72cafef )
Max tamaño:	4.8 G ( p92juprr )
```

**arcinfo** *usuario*: Muestra información de archivado de un usuario. La salida es como esta:
```
(Cmd) arcinfo i72cafef
+------------------+------------------+------------------+------------------+
| TARNAME          | SIZE             | ORIGSIZE         | FILES            |
+------------------+------------------+------------------+------------------+
| i72cafef@homenfs | 3.8 M            | 7.5 M            | 1426             |
| @SINFECHACADUCID |                  |                  |                  |
| AD.tar.bz2       |                  |                  |                  |
+------------------+------------------+------------------+------------------+
| i72cafef@homemai | 137.7 M          | 609.2 M          | 98470            |
| l=0_MAIL_MOVIDO_ |                  |                  |                  |
| 20080508@SINFECH |                  |                  |                  |
| ACADUCIDAD.tar.b |                  |                  |                  |
| z2               |                  |                  |                  |
+------------------+------------------+------------------+------------------+
| i72cafef@homecif | 111.0 B          | 0.0 B            | 1                |
| s@SINFECHACADUCI |                  |                  |                  |
| DAD.tar.bz2      |                  |                  |                  |
+------------------+------------------+------------------+------------------+
| i72cafef@homemai | 69.5 M           | 291.0 M          | 34815            |
| l=0_MOVIDOS_INAC |                  |                  |                  |
| TIVOS_20100426@S |                  |                  |                  |
| INFECHACADUCIDAD |                  |                  |                  |
| .tar.bz2         |                  |                  |                  |
+------------------+------------------+------------------+------------------+
| i72cafef@homemai | 109.8 M          | 121.8 M          | 19               |
| l=0_BORRADOS_201 |                  |                  |                  |
| 20620@SINFECHACA |                  |                  |                  |
| DUCIDAD.tar.bz2  |                  |                  |                  |
+------------------+------------------+------------------+------------------+
```

**sql** *short|long* *consulta*: Permite ejecutar una consulta sql directamente contra sigu. El primer parámetro especifica el formato short|long. En short hace la tabla visualizable en 80 columnas, en long usa todo el largo que necesite, por ejemplo para short:
```
(Cmd) sql short select * from ut_st_storage where ccuenta='i72cafef'
+----------+----------+----------+----------+----------+----------+----------+
| IDSESION | CCUENTA  |   TTAR   |  NSIZE   | CESTADO  | NSIZE_OR | NFICHERO |
|          |          |          |          |          |  IGINAL  |    S     |
+==========+==========+==========+==========+==========+==========+==========+
| 10       | i72cafef | /nfs/SIG | 4019088  | 2        | 7825485  | 1426     |
|          |          | UCLEAN/S |          |          |          |          |
|          |          | INFECHAC |          |          |          |          |
|          |          | ADUCIDAD |          |          |          |          |
|          |          | /i72cafe |          |          |          |          |
|          |          | f/i72caf |          |          |          |          |
|          |          | ef@homen |          |          |          |          |
|          |          | fs@SINFE |          |          |          |          |
|          |          | CHACADUC |          |          |          |          |
|          |          | IDAD.tar |          |          |          |          |
|          |          | .bz2     |          |          |          |          |
+----------+----------+----------+----------+----------+----------+----------+
| 10       | i72cafef | /nfs/SIG | 1.444e+0 | 2        | 6.388e+0 | 98470    |
|          |          | UCLEAN/S | 8        |          | 8        |          |
|          |          | INFECHAC |          |          |          |          |
|          |          | ADUCIDAD |          |          |          |          |
|          |          | /i72cafe |          |          |          |          |
|          |          | f/i72caf |          |          |          |          |
|          |          | ef@homem |          |          |          |          |
|          |          | ail=0_MA |          |          |          |          |
|          |          | IL_MOVID |          |          |          |          |
|          |          | O_200805 |          |          |          |          |
|          |          | 08@SINFE |          |          |          |          |
|          |          | CHACADUC |          |          |          |          |
|          |          | IDAD.tar |          |          |          |          |
|          |          | .bz2     |          |          |          |          |
+----------+----------+----------+----------+----------+----------+----------+
| 10       | i72cafef | /nfs/SIG | 111      | 2        | 0        | 1        |
|          |          | UCLEAN/S |          |          |          |          |
|          |          | INFECHAC |          |          |          |          |
|          |          | ADUCIDAD |          |          |          |          |
|          |          | /i72cafe |          |          |          |          |
|          |          | f/i72caf |          |          |          |          |
|          |          | ef@homec |          |          |          |          |
|          |          | ifs@SINF |          |          |          |          |
|          |          | ECHACADU |          |          |          |          |
|          |          | CIDAD.ta |          |          |          |          |
|          |          | r.bz2    |          |          |          |          |
+----------+----------+----------+----------+----------+----------+----------+
| 10       | i72cafef | /nfs/SIG | 72906806 | 2        | 3.051e+0 | 34815    |
|          |          | UCLEAN/S |          |          | 8        |          |
|          |          | INFECHAC |          |          |          |          |
|          |          | ADUCIDAD |          |          |          |          |
|          |          | /i72cafe |          |          |          |          |
|          |          | f/i72caf |          |          |          |          |
|          |          | ef@homem |          |          |          |          |
|          |          | ail=0_MO |          |          |          |          |
|          |          | VIDOS_IN |          |          |          |          |
|          |          | ACTIVOS_ |          |          |          |          |
|          |          | 20100426 |          |          |          |          |
|          |          | @SINFECH |          |          |          |          |
|          |          | ACADUCID |          |          |          |          |
|          |          | AD.tar.b |          |          |          |          |
|          |          | z2       |          |          |          |          |
+----------+----------+----------+----------+----------+----------+----------+
| 10       | i72cafef | /nfs/SIG | 1.151e+0 | 2        | 1.278e+0 | 19       |
|          |          | UCLEAN/S | 8        |          | 8        |          |
|          |          | INFECHAC |          |          |          |          |
|          |          | ADUCIDAD |          |          |          |          |
|          |          | /i72cafe |          |          |          |          |
|          |          | f/i72caf |          |          |          |          |
|          |          | ef@homem |          |          |          |          |
|          |          | ail=0_BO |          |          |          |          |
|          |          | RRADOS_2 |          |          |          |          |
|          |          | 0120620@ |          |          |          |          |
|          |          | SINFECHA |          |          |          |          |
|          |          | CADUCIDA |          |          |          |          |
|          |          | D.tar.bz |          |          |          |          |
|          |          | 2        |          |          |          |          |
+----------+----------+----------+----------+----------+----------+----------+
```

**ignorearchived** *True|False*: Cambia el modificador global de ignorar o no los ya archivados. "ignorearchived True" sería el equivalente a haber invocado siguclean en modo interactivo con la opción --ignore-archived. Si parámetros nos dice el estado en el que está.

**checkaltdir** *directorio*: Chequea y ofrece estadísticas de directorios alternativos apra un directorio raiz dado. Esto genera dos ficheros en /tmp: multi-movidos con aquellos usuarios que tienen varios diractorios en los alternativos y single-movidos para los usuarios que solo tienen un alternativo. Es útil para saber como va el estado de limpia de los directorios alternativos. Llegará un momento en que no sea necesario ejecutarla cuando todos los directorios alternativos hayan sido vaciados.

**sesinfo**: Muestra estadísticas de todas las sesiones:

```
(Cmd) sesinfo
+-------------------+----+-------+-------+---------+---------+---------+-------+
|      DSESION      | ID | USERS | TARS  |  FILES  |  OSIZE  |  SIZE   | %COMP |
+===================+====+=======+=======+=========+=========+=========+=======+
| PRUEBABETA        | 1  | 484   | 2343  | 269976  | 10.7 G  | 6.7 G   | 63.0  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| PRUEBABETA1       | 2  | 410   | 1948  | 325047  | 10.6 G  | 6.8 G   | 64.3  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| NOV2004DIC2005    | 3  | 1769  | 8354  | 1064263 | 37.5 G  | 22.6 G  | 60.2  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| ENE2006DIC2007    | 4  | 3036  | 13373 | 1050834 | 49.9 G  | 34.0 G  | 68.1  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| ENE2008DIC2008    | 5  | 2799  | 11390 | 836125  | 42.3 G  | 27.5 G  | 65.0  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| ENE2009DIC2009    | 6  | 3791  | 14164 | 944472  | 51.8 G  | 32.5 G  | 62.8  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| ENE2010DIC2010    | 7  | 4503  | 14918 | 1031663 | 97.1 G  | 72.8 G  | 75.0  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| 3USERS            | 8  | 3     | 9     | 311     | 49.8 M  | 22.8 M  | 45.7  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| ENE2011DIC2011    | 9  | 5116  | 16758 | 1654515 | 134.2 G | 95.7 G  | 71.3  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| SINFECHACADUCIDAD | 10 | 9818  | 40638 | 5855822 | 166.6 G | 105.3 G | 63.2  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
| ENE2012DIC2012    | 11 | 2739  | 10736 | 1841024 | 146.1 G | 100.1 G | 68.5  |
+-------------------+----+-------+-------+---------+---------+---------+-------+
```

**quit**: Sale del programa

Ejemplos de uso
---------------

Archivar todos los usuarios expirados en el año 2011 en una sesión con nombre *2011*. La sesión se creará en */nfs/SIGUCLEAN*. Las ubicaciones origen se buscarán de las particiones montadas y definidas en la tabla de montajes, excluyendo aquellas que contengan */nfs/* en su path.
```
siguclean -f 2010-01-01 -t 2010-12-31 -n 2011 --session-dir /nfs/SIGUCLEAN -x /nfs/
```

Archivar todos los usuarios especificados en el fichero *usuarios* en la sesión *VARIOS.*
```
siguclean --from-file usuarios -n VARIOS --session-dir /nfs/SIGUCLEAN -x /nfs/
```
Restaurar la sesión *FALLO* que se encuentra en */nfs/SIGUCLEAN*
```
siguclean --restore  -n FALLO --session-dir /nfs/SIGUCLEAN
```
Esta sería la forma más estándar de lanzar una sesión de archivado. Se especifican las passwords de AD y SIGU. Se colocan las fechas de selección, el nombre de la sesión, el directorio para almacenar la sesión. el filtro de exclusión de filesystems y el prefijo para ubicaciones alternativas (*0_* en este caso). Aparte activamos el debug, que pida confirmación una vez identificados los *fs* origen antes de empezar con el procesamiento, que calcule automáticamente el tamaño máximo de la sesión en función del espacio en disco, que relaje el chequeo de mandatory para usuarios cancelados y que nos muestre la barra de progreso en lugar de mensajes por pantalla (en cualquier caso los tendremos en alloutput y el resto de ficheros de salida).
```
siguclean -sigu-password XXXXXX --win-password YYYYYY -f 2012-01-01 -t 2012-12-31 -n ENE2012DIC2012 --debug --maxsize auto --confirm --sessiondir /nfs/SIGUCLEAN -x /nfs/ -p 0_ --mandatory-relax 2 --progress
```
### Caso de uso para una sesión que paramos por un problema sobrevenido###

Imaginemos que comprobamos en la salida de siguclean que no se están archivando todos los storages alternativos de los usuarios, ya sea porque no están montadas las ubiaciones alternativas, porque no se han enlazado dentro de las principales o porque no hemos especificado (o hemos especificado mal) el prefijo para ubicaciones alternativas como sería este caso:
```
siguclean -sigu-password XXXXXX --win-password YYYYYY -f 2012-01-01 -t 2012-12-31 -n ENE2012DIC2012 --debug --maxsize auto --confirm --sessiondir /nfs/SIGUCLEAN -x /nfs/ --mandatory-relax 2 --progress -p 1_
```
Hemos especificado **-p 1_** cuando nuestros enlaces a las ubicaciones alternativas comienzan por **0_**. Esto ocasionará que se queden muchas ubicaciones sin explorar. Además cuando nos damos cuenta ya tenemos archivados bastantes usuarios.

Lo primero que tendriamos que hacer es detener el procesamiento mediante la creación del fichero *STOP* en la raiz del directorio de la sesión. Una vez detenido lanzariamos una sesión de consolidación de la anterior sobreescribiendo el parámetro defectuoso, de esta manera:
```
siguclean -sigu-password XXXXXX --win-password YYYYYY --n ENE2012DIC2012 --debug  --confirm --sessiondir /nfs/SIGUCLEAN -x /nfs/ --progress -p 0_
```
Como vemos hemos obviado las opciones de selección pues el conjunto de usuarios en una sesión de consolidación se genera mediante las carpetas de archivado de usuario previamente creadas. Tampoco tiene sentido usar *--maxsize* pues hay que consolidar todos los usuarios, por lo que deberemos asegurarnos de tener especio suficiente en */NFS/SIGUCLEAN*. La relajación del mandatory tampoco tiene sentido ya que ese parámetro afecta a una selección de sigu solamente.

Una vez completado tendremos correctamente archivados todos los usuarios que se procesaron hasta el *STOP*. Por tanto ahora podemos restaurar la sesión. En este caso particular no contemplo poder sobreescribir el parámetro *-p* con el valor correcto. No tiene sentido ya que fue un error que no debería haberse producido. Por tanto editaremos previamente el fichero *cmdline* del directorio *logs* para arreglarlo (poniendo *-p 0_* en nuestro caso), y lanzamos la sesión de restauración:
```
siguclean --n ENE2012DIC2012 --sessiondir /nfs/SIGUCLEAN --restore
```
No es necesario ninguna opción más. La sesión de restauración usará las mismas opciones de la sesión original. Solo son necesarios los parámetros que nos permiten identificar de qué sesión se trata y donde está ubicada. Al terminar la sesión de restauración, el resultado será el mismo que si hubiéramos especificado correctamente la opción *-p* desde el principio.

Lógicamente si el problema venía por defecto de montajes de ubicaciones alternativas o por un fallo en la programación, deberemos arreglarlo previamente. En estos casos no sería necesario sobreescribir el parámetro *-p* ni editarlo del fichero *cmdline*.

Como buena práctica, podemos asegurarnos de que el problema se ha arreglado lanzando una sesión de prueba con la opción *--dry-run* antes de la de consolidación. Podemos detenerla cuando comprobemos que va bien (o dejarla terminar), y después lanzar el restore. La sesión dry-run en nuestro caso sería así:
```
siguclean -sigu-password XXXXXX --win-password YYYYYY -f 2012-01-01 -t 2012-12-31 -n PRUEBA-ENE2012DIC2012 --debug --maxsize auto --confirm --sessiondir /nfs/SIGUCLEAN -x /nfs/ --mandatory-relax 2 --progress -p 0_
```
Una vez terminado borramos el directorio */nfs/SIGUCLEAN/PRUEBA-ENE2012DIC2012* y lanzamos nuestra sesión de consolidación y la de restore como he especificado antes.

### Buenas prácticas ###

El lanzamiento de una sesión de prueba con *--dry-run** es conveniente hacerlo siempre antes de lanzar la sesión de archivado. Debemos tener en cuenta que puede transcurrir mucho tiempo entre que lanzamos el último archivado, y en este tiempo pueden haber cambiado muchas cosas (haber quitado montajes, algún cambio en sigu que afecte a siguclean, etc). La sesión de prueba nos dará una imagen de como se va a desarrollar el archivado, los problemas generales que ocurrirán y los usuarios que inicialmente seguro que fallarán junto sus causas (en el fichero *failreason*).

De esta forma podemos arreglar todo lo incorrecto y evaluar si conviene intentar arreglar los problemas individuales con los usuarios que van a fallar. También podemos probar que resultados obtendriamos usando opciones de relajación.

Yo no recomiendo abusar de las opciones de relajación en primera instancia. El *--mandatory-relax 3* solo deberiamos usarlo cuando queremos archivar si o si pocos usuarios que hemos comprobado que son antiguos, tienen ausencia de mandatorys identificada, etc.

A estos efectos en cualquier momento podemos lanzar sesiones "escoba" que procesen todos los usuarios que hayan quedado sin procesar. Un ejemplo sería:
```
siguclean -sigu-password XXXXXX --win-password YYYYYY -f 1900-01-01 -t 2012-12-31 -n RESTOHASTA2012 --debug --maxsize auto --confirm --sessiondir /nfs/SIGUCLEAN -x /nfs/ --mandatory-relax 3 --ldap-relax --progress -p 0_
```
Esta sesión intentará archivar todos los usuario que fallaran anteriormente con el mayor nivel de relajación hasta final de 2012. Si aparte hemos arreglado las inconsistencias de los usuarios que fallaron en sigu, es muy posible que podamos archivar el 100% de los usuarios en dicho periodo.

¿Porqué entonces no usamos niveles de relajación altos desde el primer momento? Pues porque una sesión de archivado puede durar muchas horas y en ese tiempo pueden pasar muchas cosas (que el servidor nfs se caiga, que se desmonte una ubicación, etc). La máxima tiene que ser que un usuario **se archive completamente o no se archive**. Por tanto para grandes selecciones de usuario evitaremos en la medida de lo posible relajar el chequeo. Para sesiones "escoba" que procesarán proporcionalmente pocos usuarios si las podremos usar.
