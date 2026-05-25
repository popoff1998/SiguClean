#!/usr/bin/env python3
"""
Resumen ejecutivo - Estado de Siguclean en Rocky Linux 9.3
"""

def print_section(title):
    print(f"\n{'═' * 70}")
    print(f"  {title}")
    print('═' * 70)

def main():
    print_section("SIGUCLEAN - RESUMEN EJECUTIVO")
    
    print("""
✓ MIGRACIÓN COMPLETADA
  • Python 2.7 → 3.9
  • Todos los archivos compilados sin errores
  • 50+ print statements convertidos
  • Exception handling actualizado
  • 7 dependencias Python verificadas

✓ SISTEMA PREPARADO  
  • Rocky Linux 9.6 detectado
  • Oracle Client 8.3.0 instalado
  • libclntsh.so disponible
  • Dependencias del SO instaladas (libaio)
  • Permisos root confirmados

✓ CÓDIGO MEJORADO
  • Mejor manejo de errores en checkOracleConnection()
  • Verificación de conexión antes de usar cursor
  • Inicialización automática en modo interactivo
""")
    
    print_section("COMANDOS RÁPIDOS")
    
    commands = [
        ("Verificar sistema", "python3 /opt/siguclean/verify_rocky.py"),
        ("Verificar dependencias", "python3 /opt/siguclean/check_dependencies.py"),
        ("Test Oracle", "python3 /opt/siguclean/test_quick.py"),
        ("Ejecutar Siguclean", "python3 /opt/siguclean/siguclean.py -i --sigu-password <pass>"),
    ]
    
    for desc, cmd in commands:
        print(f"\n{desc}:")
        print(f"  $ {cmd}")
    
    print_section("DOCUMENTACIÓN DISPONIBLE")
    
    docs = [
        ("ESTADO.md", "Estado completo y troubleshooting"),
        ("INSTALAR_RAPIDO.md", "Guía rápida de instalación"),
        ("ROCKY_LINUX_9_ORACLE.md", "Guía detallada para Rocky Linux"),
        ("INSTALAR_ORACLE_CLIENT.md", "Guía por sistema operativo"),
        ("requirements.txt", "Dependencias Python"),
    ]
    
    for doc, desc in docs:
        print(f"\n• {doc}")
        print(f"  {desc}")
    
    print_section("PRÓXIMOS PASOS")
    
    print("""
1. VERIFICAR SISTEMA
   $ python3 /opt/siguclean/verify_rocky.py
   
   Debería mostrar:
   ✓ Sistema Rocky Linux
   ✓ Python 3.9.x
   ✓ Todas las dependencias
   ✓ Oracle Client
   ✓ Permisos root

2. USAR SIGUCLEAN
   $ python3 /opt/siguclean/siguclean.py -i --sigu-password contraseña
   
   Comandos:
   (Cmd) help         - Ver ayuda
   (Cmd) stats        - Estadísticas (requiere Oracle)
   (Cmd) add /ruta    - Agregar ruta
   (Cmd) process      - Procesar
   (Cmd) quit         - Salir

3. SOLUCIONAR PROBLEMAS
   Ver [ESTADO.md](ESTADO.md#-problemas-comunes) para:
   • DPI-1047 (libclntsh no encontrada)
   • AttributeError (conexión Oracle fallida)
   • ORA-12514 (servicio no disponible)
""")
    
    print_section("INFORMACIÓN DEL SISTEMA")
    
    info = {
        "SO": "Rocky Linux 9.6",
        "Python": "3.9.21",
        "cx-Oracle": "8.3.0",
        "Oracle Server": "ora-av10g.bbdd.uco.es/av10g",
        "Oracle User": "sigu",
        "Oracle Client": "/opt/oracle/instantclient_*",
    }
    
    for key, value in info.items():
        print(f"\n{key:20} {value}")
    
    print("\n" + "═" * 70)
    print("  ¡LISTA PARA USAR!")
    print("═" * 70 + "\n")

if __name__ == '__main__':
    main()
