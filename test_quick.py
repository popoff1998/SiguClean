#!/usr/bin/env python3
"""
Test directo de conexión Oracle usando configuración de Siguclean
"""

import sys
import os

# Agregar directorio actual al path
sys.path.insert(0, '/opt/siguclean')

def test_import():
    """Prueba importar cx_Oracle"""
    print("1. Probando importación de cx_Oracle...")
    try:
        import cx_Oracle
        print(f"   ✓ cx_Oracle {cx_Oracle.__version__} importado")
        return True
    except ImportError as e:
        print(f"   ✗ Error: {e}")
        return False

def test_config():
    """Prueba cargar configuración"""
    print("\n2. Cargando configuración...")
    try:
        import config
        print(f"   ✓ Configuración cargada")
        print(f"   - Servidor: {config.ORACLE_SERVER}")
        print(f"   - Usuario: sigu")
        print(f"   - Contraseña: {'*' * 10 if config.ORACLE_PASS else '(sin configurar)'}")
        return config
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return None

def test_environment():
    """Prueba variables de entorno"""
    print("\n3. Variables de entorno...")
    lib_path = os.environ.get('LD_LIBRARY_PATH', '(no configurada)')
    oracle_home = os.environ.get('ORACLE_HOME', '(no configurada)')
    
    print(f"   - LD_LIBRARY_PATH: {lib_path}")
    print(f"   - ORACLE_HOME: {oracle_home}")
    
    # Buscar libclntsh
    print("\n   Buscando libclntsh...")
    import subprocess
    result = subprocess.run(['ldconfig', '-p'], capture_output=True, text=True)
    if 'libclntsh' in result.stdout:
        lines = [l for l in result.stdout.split('\n') if 'libclntsh' in l]
        for line in lines[:3]:
            print(f"   ✓ {line.strip()}")
    else:
        print("   ✗ libclntsh no encontrado")

def test_connection(config):
    """Prueba conexión a Oracle"""
    print("\n4. Probando conexión a Oracle...")
    
    if not config.ORACLE_PASS:
        print("   ⚠ Contraseña no configurada")
        print("   Usa: python3 siguclean.py -i --sigu-password <contraseña>")
        return False
    
    import cx_Oracle
    
    connection_string = f"sigu/{config.ORACLE_PASS}@{config.ORACLE_SERVER}"
    
    try:
        print(f"   Conectando a: {config.ORACLE_SERVER}")
        print(f"   Usuario: sigu")
        
        conn = cx_Oracle.connect(connection_string)
        print(f"   ✓ Conexión exitosa")
        
        # Prueba simple
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM DUAL")
        result = cursor.fetchone()
        print(f"   ✓ Query ejecutado: SELECT 1 FROM DUAL")
        
        cursor.close()
        conn.close()
        print(f"   ✓ Desconectado")
        
        return True
        
    except cx_Oracle.DatabaseError as e:
        error_obj = e.args[0]
        print(f"   ✗ Error de Base de Datos: {error_obj.message}")
        return False
    except Exception as e:
        print(f"   ✗ Error: {e}")
        return False

def main():
    print("=" * 70)
    print("  TEST DE CONEXIÓN ORACLE - SIGUCLEAN")
    print("=" * 70)
    
    # Test 1: Importación
    if not test_import():
        return 1
    
    # Test 2: Configuración
    config = test_config()
    if not config:
        return 1
    
    # Test 3: Entorno
    test_environment()
    
    # Test 4: Conexión
    if not test_connection(config):
        print("\n" + "=" * 70)
        print("SOLUCIÓN: Verifica las credenciales y el servidor Oracle")
        print("=" * 70)
        return 1
    
    print("\n" + "=" * 70)
    print("✓ TODO OK - Puedes usar Siguclean")
    print("=" * 70)
    return 0

if __name__ == '__main__':
    sys.exit(main())
