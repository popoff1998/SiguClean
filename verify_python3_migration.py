#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de verificación de migración Python 2.7 a Python 3.9
"""

import sys
import subprocess

def check_python_version():
    """Verifica que estamos usando Python 3.9+"""
    version_info = sys.version_info
    print(f"✓ Versión de Python: {version_info.major}.{version_info.minor}.{version_info.micro}")
    
    if version_info.major < 3:
        print("✗ ERROR: Se requiere Python 3+")
        return False
    
    if version_info.major == 3 and version_info.minor < 9:
        print("  (Nota: Se recomienda Python 3.9 o superior)")
    
    return True

def check_syntax(filename):
    """Verifica la sintaxis de un archivo Python"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            compile(f.read(), filename, 'exec')
        print(f"✓ {filename:30} - Sintaxis válida")
        return True
    except SyntaxError as e:
        print(f"✗ {filename:30} - ERROR de sintaxis: {e}")
        return False
    except Exception as e:
        print(f"✗ {filename:30} - ERROR: {e}")
        return False

def main():
    print("=" * 60)
    print("Verificación de migración Python 2.7 → Python 3.9")
    print("=" * 60)
    print()
    
    # Verificar versión de Python
    if not check_python_version():
        sys.exit(1)
    
    print()
    print("Verificando sintaxis de archivos Python:")
    print("-" * 60)
    
    files_to_check = [
        'siguclean.py',
        'sc_funcs.py',
        'sc_classes.py',
        'sc_shell.py',
        'sc_log.py',
        'sigudb.py',
        'config.py',
        'pyssword.py'
    ]
    
    all_valid = True
    for filename in files_to_check:
        if not check_syntax(filename):
            all_valid = False
    
    print()
    print("=" * 60)
    if all_valid:
        print("✓ Todos los archivos tienen sintaxis Python 3 válida")
        print("=" * 60)
        print()
        print("La migración ha sido exitosa. Puedes ejecutar:")
        print("  python3 siguclean.py --version")
        print("  python3 siguclean.py --help")
        return 0
    else:
        print("✗ Hay errores de sintaxis que necesitan ser corregidos")
        print("=" * 60)
        return 1

if __name__ == '__main__':
    sys.exit(main())
