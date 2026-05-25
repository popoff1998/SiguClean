#!/bin/bash
# Script de instalación automática de Oracle Client para Rocky Linux 9.3
# Uso: bash install_oracle_rocky.sh

set -e

echo "════════════════════════════════════════════════════════════════"
echo "  Instalación de Oracle Client para Rocky Linux 9.3"
echo "════════════════════════════════════════════════════════════════"
echo

# Verificar que se ejecuta como root o con sudo
if [ "$EUID" -ne 0 ]; then
   echo "Este script debe ejecutarse con sudo"
   echo "Uso: sudo bash install_oracle_rocky.sh"
   exit 1
fi

# Paso 1: Verificar versión de Rocky Linux
echo "1. Verificando sistema operativo..."
if [ -f /etc/redhat-release ]; then
    OS_VERSION=$(cat /etc/redhat-release)
    echo "   ✓ Sistema detectado: $OS_VERSION"
else
    echo "   ✗ Este script solo funciona en Red Hat/CentOS/Rocky"
    exit 1
fi

# Paso 2: Instalar dependencias
echo
echo "2. Instalando dependencias del sistema..."
if command -v dnf &> /dev/null; then
    echo "   Usando dnf..."
    dnf install -y libaio unzip wget
elif command -v yum &> /dev/null; then
    echo "   Usando yum..."
    yum install -y libaio unzip wget
else
    echo "   ✗ No se encontró dnf ni yum"
    exit 1
fi
echo "   ✓ Dependencias instaladas"

# Paso 3: Crear directorio Oracle
echo
echo "3. Creando directorio /opt/oracle..."
mkdir -p /opt/oracle
chmod 755 /opt/oracle
echo "   ✓ Directorio creado"

# Paso 4: Verificar Oracle Instant Client
echo
echo "4. Verificando Oracle Instant Client..."
ORACLE_ZIP=""

# Buscar archivo zip en directorios comunes
for dir in /tmp /root /home/*; do
    if [ -f "$dir/instantclient-basiclite-linux.x86_64-*.zip" 2>/dev/null ]; then
        ORACLE_ZIP=$(ls -t "$dir/instantclient-basiclite-linux.x86_64-"*.zip 2>/dev/null | head -1)
        break
    fi
done

if [ -z "$ORACLE_ZIP" ]; then
    echo "   ⚠ No se encontró instantclient-basiclite-linux.x86_64-*.zip"
    echo
    echo "   Descarga desde:"
    echo "   https://www.oracle.com/database/technologies/instant-client/linux-x86-64-downloads.html"
    echo
    echo "   Luego ejecuta:"
    echo "   sudo bash install_oracle_rocky.sh"
    exit 1
else
    echo "   ✓ Archivo encontrado: $ORACLE_ZIP"
fi

# Paso 5: Extraer archivos
echo
echo "5. Extrayendo Oracle Instant Client..."
cd /tmp
unzip -q "$ORACLE_ZIP"
echo "   ✓ Archivos extraídos"

# Paso 6: Copiar a /opt/oracle
echo
echo "6. Copiando a /opt/oracle..."
INSTANTCLIENT_DIR=$(ls -d instantclient_* 2>/dev/null | head -1)
if [ -z "$INSTANTCLIENT_DIR" ]; then
    echo "   ✗ No se encontró directorio instantclient_*"
    exit 1
fi

cp -r "$INSTANTCLIENT_DIR" /opt/oracle/
chmod -R 755 /opt/oracle/"$INSTANTCLIENT_DIR"
echo "   ✓ Copiado a /opt/oracle/$INSTANTCLIENT_DIR"

# Paso 7: Crear enlaces simbólicos
echo
echo "7. Creando enlaces simbólicos..."
cd /opt/oracle/"$INSTANTCLIENT_DIR"

# Encontrar la versión correcta de libclntsh
LIBCLNTSH=$(ls -1 libclntsh.so.* 2>/dev/null | head -1)
if [ -z "$LIBCLNTSH" ]; then
    echo "   ✗ No se encontró libclntsh.so.*"
    exit 1
fi

ln -sf "$LIBCLNTSH" libclntsh.so
echo "   ✓ Enlace simbólico creado: $LIBCLNTSH -> libclntsh.so"

# Paso 8: Configurar LD_LIBRARY_PATH
echo
echo "8. Configurando LD_LIBRARY_PATH..."
ORACLE_PATH="/opt/oracle/$INSTANTCLIENT_DIR"

# Crear archivo de configuración para ldconfig
cat > /etc/ld.so.conf.d/oracle.conf << EOF
$ORACLE_PATH
EOF

# Recargar cache de ldconfig
ldconfig -v 2>/dev/null | grep -E "libclntsh|oracle" || true
echo "   ✓ Configuración actualizada"

# Paso 9: Verificar instalación
echo
echo "9. Verificando instalación..."
if ldconfig -p | grep -q libclntsh; then
    echo "   ✓ libclntsh.so está disponible"
    ldconfig -p | grep libclntsh | head -2
else
    echo "   ✗ libclntsh.so no está disponible"
    echo "   Intenta ejecutar: sudo ldconfig -v"
    exit 1
fi

# Paso 10: Test de Python
echo
echo "10. Probando con Python..."
python3 << PYEOF
try:
    import cx_Oracle
    print("    ✓ cx_Oracle importado correctamente")
except ImportError as e:
    print(f"    ✗ Error: {e}")
    exit(1)
PYEOF

echo
echo "════════════════════════════════════════════════════════════════"
echo "✓ INSTALACIÓN COMPLETADA"
echo "════════════════════════════════════════════════════════════════"
echo
echo "Próximos pasos:"
echo
echo "1. Probar conexión a Oracle:"
echo "   python3 /opt/siguclean/test_oracle_connection.py"
echo
echo "2. Ejecutar Siguclean:"
echo "   python3 /opt/siguclean/siguclean.py -i --sigu-password tu_contraseña"
echo
echo "3. Verificar comando stats:"
echo "   (Cmd) stats"
echo
