#!/bin/bash
# Script rápido para ejecutar Siguclean en Rocky Linux 9.3

if [ $# -eq 0 ]; then
    echo "Uso: bash run_siguclean.sh <contraseña_sigu>"
    echo ""
    echo "Ejemplo:"
    echo "  bash run_siguclean.sh micontraseña"
    echo ""
    echo "Modo batch:"
    echo "  bash run_siguclean.sh <contraseña> batch /ruta/a/procesar"
    exit 1
fi

PASSWORD="$1"
MODE="${2:-interactive}"
BATCH_PATH="${3:-.}"

echo "════════════════════════════════════════════════════════════════"
echo "  SIGUCLEAN en Rocky Linux 9.3"
echo "════════════════════════════════════════════════════════════════"
echo

# Verificación rápida
echo "Verificando sistema..."
python3 << EOF
import sys
sys.path.insert(0, '/opt/siguclean')

# Verificar Python
print(f"✓ Python {sys.version.split()[0]}")

# Verificar cx_Oracle
try:
    import cx_Oracle
    print(f"✓ cx-Oracle {cx_Oracle.__version__}")
except ImportError:
    print("✗ cx-Oracle no disponible")
    sys.exit(1)

# Verificar config
try:
    import config
    print(f"✓ Configuración cargada")
except:
    print("✗ Error cargando configuración")
    sys.exit(1)

print()
EOF

if [ $? -ne 0 ]; then
    echo "Error: Sistema no listo"
    exit 1
fi

# Ejecutar
if [ "$MODE" = "interactive" ]; then
    echo "Iniciando modo interactivo..."
    echo "(Cmd) help              - Ver ayuda"
    echo "(Cmd) stats             - Ver estadísticas"
    echo "(Cmd) add /ruta         - Agregar ruta"
    echo "(Cmd) process           - Procesar"
    echo "(Cmd) quit              - Salir"
    echo
    python3 /opt/siguclean/siguclean.py -i --sigu-password "$PASSWORD"
elif [ "$MODE" = "batch" ]; then
    echo "Iniciando modo batch en: $BATCH_PATH"
    python3 /opt/siguclean/siguclean.py -b "$BATCH_PATH" --sigu-password "$PASSWORD"
else
    echo "Modo desconocido: $MODE"
    exit 1
fi
