#!/usr/bin/env python3
"""
Script de verificación previa para Rocky Linux 9.3
Comprueba que todo esté listo antes de instalar Oracle Client
"""

import os
import sys
import subprocess
import platform

def print_header(text):
    print(f"\n{'═' * 60}")
    print(f"  {text}")
    print('═' * 60)

def check_system():
    """Verifica SO y versión"""
    print_header("1. Verificación del Sistema")
    
    system = platform.system()
    print(f"Sistema: {system}")
    
    if system != "Linux":
        print("✗ Este script solo funciona en Linux")
        return False
    
    # Detectar distribución
    try:
        with open('/etc/redhat-release', 'r') as f:
            distro = f.read().strip()
            print(f"Distribución: {distro}")
            if "Rocky" not in distro:
                print("⚠ Se recomienda Rocky Linux")
            return True
    except FileNotFoundError:
        print("✗ No parece ser Rocky Linux / RedHat")
        return False

def check_python():
    """Verifica versión de Python"""
    print_header("2. Verificación de Python")
    
    version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"Versión Python: {version}")
    
    if sys.version_info.major < 3:
        print("✗ Se requiere Python 3.x")
        return False
    
    if sys.version_info.minor < 7:
        print("⚠ Se recomienda Python 3.7 o superior")
    
    return True

def check_dependencies():
    """Verifica dependencias instaladas"""
    print_header("3. Verificación de Dependencias Python")
    
    required = {
        'dateutil': 'python-dateutil',
        'tenacity': 'tenacity',
        'cx_Oracle': 'cx-Oracle',
        'ldap': 'python-ldap',
        'progressbar': 'progressbar33',
        'texttable': 'texttable',
    }
    
    all_ok = True
    for module, package in required.items():
        try:
            __import__(module)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} - No instalado")
            all_ok = False
    
    return all_ok

def check_system_packages():
    """Verifica paquetes del sistema"""
    print_header("4. Verificación de Paquetes del Sistema")
    
    # Verificar libaio
    try:
        subprocess.run(['ldconfig', '-p'], 
                      capture_output=True, 
                      check=False)
        result = subprocess.run(['ldconfig', '-p'], 
                              capture_output=True, 
                              text=True, 
                              check=False)
        if 'libaio' in result.stdout:
            print("✓ libaio instalado")
            libaio_ok = True
        else:
            print("✗ libaio no encontrado")
            print("  Instala con: sudo dnf install -y libaio")
            libaio_ok = False
    except Exception as e:
        print(f"⚠ Error verificando libaio: {e}")
        libaio_ok = False
    
    # Verificar herramientas
    tools = ['unzip', 'wget', 'ldconfig']
    all_ok = True
    for tool in tools:
        try:
            subprocess.run(['which', tool], 
                         capture_output=True, 
                         check=True)
            print(f"✓ {tool} disponible")
        except subprocess.CalledProcessError:
            print(f"✗ {tool} no encontrado")
            print(f"  Instala con: sudo dnf install -y {tool}")
            all_ok = False
    
    return all_ok and libaio_ok

def check_oracle():
    """Verifica Oracle Instant Client"""
    print_header("5. Verificación de Oracle Instant Client")
    
    # Verificar si está instalado
    try:
        import cx_Oracle
        print(f"✓ Oracle Client encontrado")
        print(f"  Versión cx_Oracle: {cx_Oracle.__version__}")
        return True
    except ImportError as e:
        print(f"⚠ Oracle Client no está instalado")
        print(f"  Error: {e}")
    
    # Verificar si hay un zip descargado
    common_dirs = ['/tmp', os.path.expanduser('~'), '/root']
    for directory in common_dirs:
        if os.path.exists(directory):
            for file in os.listdir(directory):
                if 'instantclient' in file and file.endswith('.zip'):
                    print(f"✓ Archivo encontrado: {directory}/{file}")
                    return False
    
    print("✗ Oracle Instant Client no descargado")
    print("  Descarga desde: https://www.oracle.com/database/technologies/instant-client/")
    return False

def check_permissions():
    """Verifica permisos necesarios"""
    print_header("6. Verificación de Permisos")
    
    if os.geteuid() == 0:
        print("✓ Ejecutándose con permisos root")
        return True
    else:
        print("⚠ Este script debería ejecutarse con sudo")
        print("  Uso: sudo python3 verify_rocky.py")
        return False

def main():
    print("\n" + "═" * 60)
    print("  VERIFICACIÓN PREVIA - ROCKY LINUX 9.3")
    print("  Instalación de Oracle Client")
    print("═" * 60)
    
    results = {}
    
    results['Sistema'] = check_system()
    results['Python'] = check_python()
    results['Dependencias Python'] = check_dependencies()
    results['Paquetes Sistema'] = check_system_packages()
    results['Oracle Client'] = check_oracle()
    results['Permisos'] = check_permissions()
    
    # Resumen
    print_header("RESUMEN")
    
    for check, result in results.items():
        status = "✓" if result else "✗"
        print(f"{status} {check}")
    
    print("\n" + "═" * 60)
    if all(results.values()):
        print("✓ SISTEMA LISTO - Puedes ejecutar:")
        print("  sudo bash /opt/siguclean/install_oracle_rocky.sh")
    else:
        print("⚠ Faltan algunos requisitos - Revisa arriba")
        print("\nPasos recomendados:")
        if not results['Dependencias Python']:
            print("1. Instala dependencias Python:")
            print("   python3 -m pip install -r /opt/siguclean/requirements.txt")
        if not results['Paquetes Sistema']:
            print("2. Instala paquetes del sistema:")
            print("   sudo dnf install -y libaio unzip wget")
        if not results['Oracle Client']:
            print("3. Descarga Oracle Instant Client desde:")
            print("   https://www.oracle.com/database/technologies/instant-client/")
    
    print("═" * 60 + "\n")
    
    return 0 if all(results.values()) else 1

if __name__ == '__main__':
    sys.exit(main())
