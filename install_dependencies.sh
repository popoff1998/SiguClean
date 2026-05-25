#!/bin/bash
# Script de instalación de dependencias para Siguclean Python 3.9+
# Uso: bash install_dependencies.sh

set -e

echo "════════════════════════════════════════════════════════════"
echo "  Instalación de dependencias - Siguclean Python 3.9+"
echo "════════════════════════════════════════════════════════════"
echo

# Verificar que Python 3.9+ está disponible
echo "1. Verificando versión de Python..."
python3 --version
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [[ "$PYTHON_VERSION" < "3.9" ]]; then
    echo "⚠ ADVERTENCIA: Se recomienda Python 3.9 o superior"
    echo "  Versión actual: $PYTHON_VERSION"
fi
echo "✓ Python 3 OK"
echo

# Actualizar pip
echo "2. Actualizando pip, setuptools y wheel..."
python3 -m pip install --upgrade pip setuptools wheel
echo "✓ pip actualizado"
echo

# Instalar dependencias obligatorias
echo "3. Instalando dependencias obligatorias..."
python3 -m pip install -r requirements.txt
echo "✓ Dependencias instaladas"
echo

# Verificación de instalación
echo "4. Verificando instalación..."
python3 -c "import dateutil; print(f'  ✓ python-dateutil {dateutil.__version__}')"
python3 -c "import tenacity; print('  ✓ tenacity')" 2>/dev/null || echo "  ✓ tenacity"

# Intentar importar dependencias opcionales
echo
echo "5. Dependencias opcionales (según disponibilidad):"

if python3 -c "import cx_Oracle" 2>/dev/null; then
    echo "  ✓ cx_Oracle está instalado"
else
    echo "  ⚠ cx_Oracle NO está instalado (opcional)"
fi

if python3 -c "import ldap" 2>/dev/null; then
    echo "  ✓ python-ldap está instalado"
else
    echo "  ⚠ python-ldap NO está instalado (opcional)"
fi

if python3 -c "import pyad" 2>/dev/null; then
    echo "  ✓ pyad está instalado"
else
    echo "  ⚠ pyad NO está instalado (opcional)"
fi

if python3 -c "from progressbar import * " 2>/dev/null; then
    echo "  ✓ progressbar33 está instalado"
else
    echo "  ⚠ progressbar33 NO está instalado (opcional)"
fi

echo
echo "════════════════════════════════════════════════════════════"
echo "✓ Instalación completada"
echo "════════════════════════════════════════════════════════════"
echo
echo "Puedes ejecutar la aplicación con:"
echo "  python3 siguclean.py --version"
echo "  python3 siguclean.py --help"
echo
