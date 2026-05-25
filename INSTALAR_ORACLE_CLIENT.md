# Error Real: Librerías de Oracle Client No Instaladas

## Problema Identificado

El verdadero error es:
```
DPI-1047: Cannot locate a 64-bit Oracle Client library: 
"libclntsh.so: cannot open shared object file: No such file or directory"
```

**No es un error de código**, es que **las librerías de Oracle Client no están disponibles en el sistema**.

## Solución

Necesitas instalar las librerías de Oracle Client en tu servidor Linux.

### Opción 1: Instalar Oracle Instant Client (Recomendado)

#### Para Rocky Linux 9.3:
```bash
# Instalar dependencias
sudo dnf install -y libaio
# O si dnf no funciona:
sudo yum install -y libaio

# Descargar Oracle Instant Client desde:
# https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

mkdir -p /opt/oracle
unzip instantclient-basiclite-linux.x86_64-19.x.x.x.zip

# Copiar a /opt/oracle
sudo cp -r instantclient_19_* /opt/oracle/

# Crear enlace simbólico
cd /opt/oracle/instantclient_19_*
sudo ln -s libclntsh.so.19.1 libclntsh.so

# Configurar LD_LIBRARY_PATH permanentemente
sudo bash -c 'echo "/opt/oracle/instantclient_19_*" > /etc/ld.so.conf.d/oracle.conf'
sudo ldconfig
```

#### Para Ubuntu/Debian:
```bash
# Descargar Oracle Instant Client (requiere cuenta Oracle)
# O usar estos pasos:

sudo apt-get update
sudo apt-get install -y libaio1 libaio-dev

# Descargar desde https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html
# O si tienes acceso local:

# Extraer en /opt/oracle
mkdir -p /opt/oracle
unzip instantclient-basiclite-linux-x86_64-19.x.x.x.zip -d /opt/oracle/

# Crear enlaces simbólicos
cd /opt/oracle/instantclient_*
ln -s libclntsh.so.* libclntsh.so

# Configurar LD_LIBRARY_PATH
echo "export LD_LIBRARY_PATH=/opt/oracle/instantclient_*:$LD_LIBRARY_PATH" >> ~/.bashrc
source ~/.bashrc
```

#### Para CentOS/RedHat:
```bash
sudo yum install -y libaio

# Descargar desde Oracle
# Extraer en /opt/oracle
mkdir -p /opt/oracle
unzip instantclient-basiclite-linux-x86_64-19.x.x.x.zip -d /opt/oracle/

# Crear enlaces simbólicos
cd /opt/oracle/instantclient_*
ln -s libclntsh.so.* libclntsh.so

# Configurar variables de entorno
echo "export LD_LIBRARY_PATH=/opt/oracle/instantclient_*:$LD_LIBRARY_PATH" >> ~/.bashrc
source ~/.bashrc
```

### Opción 2: Usar Oracle Client Completo

Si tienes acceso a Oracle Database en tu red local:

```bash
# Instalar Oracle Client completo
# (Requiere descarga desde Oracle)

export ORACLE_HOME=/opt/oracle/product/19c/client_1
export PATH=$ORACLE_HOME/bin:$PATH
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH
```

## Verificación

Después de instalar las librerías, verifica:

```bash
# Buscar libclntsh.so
find / -name "libclntsh.so*" 2>/dev/null

# Verificar que está en la ruta
ldconfig -p | grep libclntsh

# Ejecutar el test de diagnóstico
python3 /opt/siguclean/test_oracle_connection.py
```

## Si Aún No Funciona

### Problema: "libclntsh.so not found"

```bash
# Verificar la ruta
ls -la /opt/oracle/instantclient_*/libclntsh.so*

# Crear enlace si no existe
cd /opt/oracle/instantclient_*
ln -s libclntsh.so.19.1 libclntsh.so

# Verificar LD_LIBRARY_PATH
echo $LD_LIBRARY_PATH
```

### Problema: Arquitectura de 32 vs 64 bits

```bash
# Verificar arquitectura del sistema
uname -m
# Debe devolver: x86_64 (64-bit)

# Instalar cliente correcto (siempre 64-bit para Linux x86_64)
```

### Problema: Versión de Oracle no coincide

```bash
# El cliente cx_Oracle requiere Oracle Client 11.2 o superior
# Verifica la versión

/opt/oracle/instantclient_*/sqlplus -version
```

## Alternativa: Usar Python sin Oracle (Modo Demo)

Si no puedes instalar Oracle Client, puedes usar Siguclean sin funcionalidades que requieran base de datos:

```bash
# Ejecutar sin comandos que requieran Oracle
python3 siguclean.py --help

# Ver comandos disponibles sin Oracle
python3 siguclean.py -i
(Cmd) help
```

## Recursos

- [Oracle Instant Client Downloads](https://www.oracle.com/database/technologies/instant-client/downloads.html)
- [cx_Oracle Installation Guide](https://cx-Oracle.readthedocs.io/en/latest/user_guide/installation.html)
- [Oracle Client Configuration](https://docs.oracle.com/en/database/oracle/oracle-database/19/lacli/index.html)

## Próximos Pasos

1. Instala las librerías de Oracle Client según tu sistema operativo
2. Configura `LD_LIBRARY_PATH`
3. Ejecuta el test de diagnóstico
4. Luego podrás usar Siguclean con comandos que requieren Oracle

```bash
python3 /opt/siguclean/test_oracle_connection.py
```
