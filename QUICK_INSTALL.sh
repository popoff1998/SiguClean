#!/bin/bash
# Guía rápida de instalación - Siguclean Python 3.9+

# ============================================================
# MÉTODO 1: AUTOMÁTICO (Recomendado)
# ============================================================
cd /opt/siguclean
bash install_dependencies.sh

# ============================================================
# MÉTODO 2: MANUAL CON REQUIREMENTS.TXT
# ============================================================
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt

# ============================================================
# MÉTODO 3: INSTALACIÓN INDIVIDUAL
# ============================================================

# Dependencias obligatorias
python3 -m pip install python-dateutil
python3 -m pip install tenacity

# Dependencias para conexión a Oracle
python3 -m pip install cx-Oracle

# Dependencias para LDAP
python3 -m pip install python-ldap
# En caso de error, instalar dependencias del sistema:
# - Ubuntu/Debian: sudo apt-get install libldap2-dev libsasl2-dev
# - RedHat/CentOS: sudo yum install openldap-devel cyrus-sasl-devel

# Dependencias para Active Directory
python3 -m pip install pyad

# Barra de progreso
python3 -m pip install progressbar33

# ============================================================
# VERIFICAR INSTALACIÓN
# ============================================================
python3 -c "
import sys
modules = ['dateutil', 'tenacity', 'cx_Oracle', 'ldap', 'pyad', 'progressbar']
print('Módulos instalados:')
for mod in modules:
    try:
        __import__(mod)
        print(f'  ✓ {mod}')
    except ImportError:
        print(f'  ✗ {mod} (no instalado)')
"

# ============================================================
# EJECUTAR LA APLICACIÓN
# ============================================================
python3 siguclean.py --version
python3 siguclean.py --help

# ============================================================
# ACTUALIZAR DEPENDENCIAS
# ============================================================
python3 -m pip install --upgrade -r requirements.txt

# ============================================================
# CREAR ARCHIVO DE DEPENDENCIAS CONGELADAS
# ============================================================
# (Para reproducir exactamente las versiones instaladas)
pip3 freeze > requirements-lock.txt
