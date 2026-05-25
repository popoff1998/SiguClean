# Instalación Rápida - Rocky Linux 9.3

## Pasos Rápidos

### 1. Descargar Oracle Instant Client

Visita: https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html

Descarga: **instantclient-basiclite-linux.x86_64-*.zip**

Guarda en `/tmp/` o similar.

### 2. Ejecutar Script de Instalación

```bash
sudo bash /opt/siguclean/install_oracle_rocky.sh
```

El script:
- Verifica el SO (Rocky Linux 9.3)
- Instala libaio, unzip, wget
- Busca el archivo zip automáticamente
- Extrae e instala en /opt/oracle
- Configura LD_LIBRARY_PATH
- Verifica que todo funciona

### 3. Probar Conexión Oracle

```bash
python3 /opt/siguclean/test_oracle_connection.py
```

Debería mostrar:
```
✓ Módulo cx_Oracle cargado correctamente
✓ Oracle Client Library encontrada
✓ Versión: X.X.X
```

### 4. Ejecutar Siguclean

```bash
python3 /opt/siguclean/siguclean.py -i --sigu-password tu_contraseña
```

En el prompt interactivo:
```
(Cmd) stats
```

## Si algo falla

### Error: "No se encontró instantclient-basiclite-linux.x86_64-*.zip"

Asegúrate de que:
1. Descargaste el archivo correcto
2. Está en `/tmp/` o similar
3. Ejecutas: `sudo bash /opt/siguclean/install_oracle_rocky.sh`

### Error: "DPI-1047: Cannot locate a 64-bit Oracle Client library"

Ejecuta:
```bash
sudo ldconfig -v | grep libclntsh
```

Si no aparece:
```bash
sudo ldconfig -v
cd /opt/oracle/instantclient_*
sudo bash -c 'echo "$PWD" >> /etc/ld.so.conf'
sudo ldconfig
```

### Error: "No se pudo conectar a Oracle"

1. Verifica las credenciales (usuario sigu)
2. Verifica que el servidor `ora-av10g.bbdd.uco.es` esté accesible:
```bash
ping ora-av10g.bbdd.uco.es
nslookup ora-av10g.bbdd.uco.es
```

3. Verifica el puerto 1521:
```bash
telnet ora-av10g.bbdd.uco.es 1521
```

## Información de Conexión

- **Usuario**: sigu
- **Servidor**: ora-av10g.bbdd.uco.es
- **BD**: av10g
- **Puerto**: 1521 (por defecto)

## Verificación Final

```bash
# Verificar libclntsh
ldconfig -p | grep libclntsh

# Verificar instalación Python
python3 << EOF
import cx_Oracle
print(f"cx_Oracle versión: {cx_Oracle.__version__}")
EOF

# Test de conexión (requiere credenciales)
python3 /opt/siguclean/test_oracle_connection.py
```

## Documentación Completa

Para más detalles: [ROCKY_LINUX_9_ORACLE.md](ROCKY_LINUX_9_ORACLE.md)
