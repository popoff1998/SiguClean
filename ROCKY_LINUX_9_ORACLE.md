# Instalar Oracle Client en Rocky Linux 9.3

## Paso 1: Instalar Dependencias del Sistema

```bash
sudo dnf install -y libaio
# O si lo anterior no funciona:
sudo yum install -y libaio
```

## Paso 2: Descargar Oracle Instant Client

Necesitas descargar desde Oracle. Hay tres opciones:

### Opción A: Desde Oracle directamente (requiere cuenta Oracle)

Ve a: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

Descarga:
- `instantclient-basiclite-linux.x86_64-19.x.x.x.zip` (versión 19 o superior)

### Opción B: Si ya tienes el archivo

Sube el archivo a tu servidor Rocky y continúa.

## Paso 3: Instalar Oracle Instant Client

```bash
# Crear directorio
mkdir -p /opt/oracle

# Extraer (reemplaza con tu nombre de archivo)
cd /tmp
unzip instantclient-basiclite-linux.x86_64-19.x.x.x.zip

# Copiar a /opt/oracle
sudo cp -r instantclient_19_* /opt/oracle/

# Cambiar propietario
sudo chown -R root:root /opt/oracle/instantclient_*
```

## Paso 4: Crear Enlaces Simbólicos

```bash
cd /opt/oracle/instantclient_*

# Ver qué versión de libclntsh existe
ls -la libclntsh.so*

# Crear enlace simbólico (reemplaza con tu versión)
sudo ln -s libclntsh.so.19.1 libclntsh.so
```

## Paso 5: Configurar Variables de Entorno

### Opción 1: Para el usuario actual (temporal)

```bash
export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_*:$LD_LIBRARY_PATH
```

### Opción 2: Permanente para el usuario

```bash
echo 'export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_*:$LD_LIBRARY_PATH' >> ~/.bashrc
source ~/.bashrc
```

### Opción 3: Permanente para todos los usuarios (recomendado)

```bash
sudo bash -c 'echo "/opt/oracle/instantclient_19_*" > /etc/ld.so.conf.d/oracle.conf'
sudo ldconfig
```

## Paso 6: Verificar Instalación

```bash
# Verificar que las librerías están disponibles
ldconfig -p | grep libclntsh

# Debería mostrar algo como:
# libclntsh.so.19.1 (libc6,x86-64) => /opt/oracle/instantclient_19_23/libclntsh.so.19.1
# libclntsh.so (libc6,x86-64) => /opt/oracle/instantclient_19_23/libclntsh.so
```

## Paso 7: Probar Conexión a Oracle

```bash
# Test con el script de diagnóstico
cd /opt/siguclean
python3 test_oracle_connection.py

# O test directo con Python
python3 << 'EOF'
import cx_Oracle
print("✓ cx_Oracle cargado correctamente")

# Intentar conexión (reemplaza con tus credenciales)
try:
    conn = cx_Oracle.connect('sigu/password@ora-av10g.bbdd.uco.es/av10g')
    print("✓ Conexión a Oracle exitosa")
    conn.close()
except Exception as e:
    print(f"✗ Error: {e}")
EOF
```

## Rocky Linux Específico: Alternativa con SELinux

Si tienes SELinux habilitado:

```bash
# Ver estado de SELinux
getenforce

# Si está en enforcing, ajusta los permisos
sudo semanage fcontext -a -t default_t "/opt/oracle(/.*)?"
sudo restorecon -Rv /opt/oracle
```

## Solución de Problemas

### Error: "libclntsh.so: No such file or directory"

```bash
# Buscar dónde está el archivo
find /opt/oracle -name "libclntsh.so*"

# Crear enlace si falta
cd /opt/oracle/instantclient_19_*
sudo ln -s libclntsh.so.19.1 libclntsh.so

# Recargar configuración
sudo ldconfig
```

### Error: "Can't open display"

Este error es normal si ejecutas desde terminal remota. Ignóralo.

### Error: "ORA-12514" (listener not found)

El servidor Oracle no está disponible o el nombre es incorrecto.

```bash
# Verificar conectividad
ping ora-av10g.bbdd.uco.es
tnsping ora-av10g.bbdd.uco.es

# Si tnsping no está disponible, eso es normal en Instant Client
```

### Error: "ORA-01017" (usuario/contraseña incorrectos)

Verifica tus credenciales en `config.py` o usa la opción `--sigu-password`.

## Paso Final: Usar Siguclean

```bash
cd /opt/siguclean

# Modo interactivo con contraseña
python3 siguclean.py -i --sigu-password tu_contraseña

# En el prompt
(Cmd) stats
(Cmd) help
(Cmd) exit
```

## Comandos Útiles en Rocky

```bash
# Ver versión
cat /etc/redhat-release

# Ver librerías instaladas
ldconfig -p | grep -E "libc|libaio"

# Verificar Python
python3 --version

# Verificar pip
pip3 --version
```

## Referencias para Rocky Linux 9.3

- [Rocky Linux Official Docs](https://docs.rockylinux.org/)
- [Oracle Instant Client Installation](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html)
- [Oracle Database on Linux](https://docs.oracle.com/en/database/oracle/oracle-database/21/lnxdb/)
