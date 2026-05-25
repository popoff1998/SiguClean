#!/usr/bin/env python3
"""
Script de diagnóstico para verificar conexión a Oracle
"""

import sys
import getpass

def test_oracle_connection():
    """Prueba la conexión a Oracle"""
    
    print("=" * 70)
    print("DIAGNÓSTICO DE CONEXIÓN A ORACLE")
    print("=" * 70)
    print()
    
    # Paso 1: Verificar módulo cx_Oracle
    print("1. Verificando módulo cx_Oracle...")
    try:
        import cx_Oracle
        print("   ✓ cx_Oracle importado correctamente")
    except ImportError as e:
        print(f"   ✗ Error: {e}")
        print("   Solución: pip3 install cx-Oracle")
        return False
    
    # Paso 2: Obtener credenciales
    print("\n2. Solicitando credenciales...")
    print("   Servidor Oracle (por defecto: ora-av10g.bbdd.uco.es/av10g): ", end='')
    server = input().strip()
    if not server:
        server = "ora-av10g.bbdd.uco.es/av10g"
    print(f"   → Servidor: {server}")
    
    print("   Usuario (por defecto: sigu): ", end='')
    user = input().strip()
    if not user:
        user = "sigu"
    print(f"   → Usuario: {user}")
    
    password = getpass.getpass("   Contraseña: ")
    print("   → Contraseña: (introducida)")
    
    # Paso 3: Intentar conexión
    print("\n3. Intentando conexión a Oracle...")
    try:
        dsn = f"{user}/{password}@{server}"
        print(f"   Conexión: {user}@{server}")
        connection = cx_Oracle.connect(dsn)
        print("   ✓ ¡CONEXIÓN EXITOSA!")
        
        # Paso 4: Ejecutar consulta de prueba
        print("\n4. Ejecutando consulta de prueba...")
        cursor = connection.cursor()
        try:
            cursor.execute("select count(*) from ut_cuentas")
            count = cursor.fetchone()[0]
            print(f"   ✓ Consulta exitosa - {count} cuentas en la base de datos")
        except Exception as e:
            print(f"   ⚠ La conexión funciona pero la tabla podría no existir: {e}")
        finally:
            cursor.close()
        
        connection.close()
        return True
        
    except cx_Oracle.DatabaseError as e:
        error = e.args[0]
        print(f"   ✗ Error de conexión: {error.message}")
        print("\n   Posibles causas:")
        if "ORA-12514" in str(error):
            print("   • El listener de Oracle no está escuchando")
        elif "ORA-01017" in str(error):
            print("   • Usuario o contraseña incorrectos")
        elif "ORA-12541" in str(error):
            print("   • No hay conexión con el servidor Oracle")
        else:
            print("   • Verifica servidor, usuario y contraseña")
        return False
    except Exception as e:
        print(f"   ✗ Error inesperado: {e}")
        return False

def main():
    success = test_oracle_connection()
    
    print("\n" + "=" * 70)
    if success:
        print("✓ DIAGNÓSTICO COMPLETADO - CONEXIÓN FUNCIONA")
        print("\nPuedes usar ahora:")
        print("  python3 siguclean.py -i --sigu-password tu_contraseña")
    else:
        print("✗ DIAGNÓSTICO COMPLETADO - FALLÓ LA CONEXIÓN")
        print("\nVerifica:")
        print("  1. Servidor Oracle está disponible")
        print("  2. Usuario y contraseña son correctos")
        print("  3. Cliente Oracle está instalado correctamente")
        print("  4. Las librerías de Oracle están disponibles")
    print("=" * 70)
    
    return 0 if success else 1

if __name__ == '__main__':
    sys.exit(main())
