#!/usr/bin/env python3
"""
Verificador de dependencias para Siguclean
Verifica que todas las librerías requeridas están disponibles
"""

import sys

def check_dependencies():
    """Verifica todas las dependencias externas"""
    
    dependencies = {
        'python-dateutil': {
            'module': 'dateutil',
            'category': 'Core',
            'required': True,
            'version': None
        },
        'tenacity': {
            'module': 'tenacity',
            'category': 'Core',
            'required': True,
            'version': None
        },
        'cx-Oracle': {
            'module': 'cx_Oracle',
            'category': 'Database',
            'required': False,
            'version': None
        },
        'python-ldap': {
            'module': 'ldap',
            'category': 'Database',
            'required': False,
            'version': None
        },
        'pyad': {
            'module': 'pyad',
            'category': 'Active Directory',
            'required': False,
            'version': None,
            'note': 'Solo en Windows'
        },
        'progressbar33': {
            'module': 'progressbar',
            'category': 'UI',
            'required': False,
            'version': None
        },
        'texttable': {
            'module': 'texttable',
            'category': 'UI',
            'required': False,
            'version': None
        }
    }
    
    print("=" * 70)
    print("VERIFICADOR DE DEPENDENCIAS - SIGUCLEAN")
    print("=" * 70)
    print()
    
    missing_required = []
    missing_optional = []
    
    categories = {}
    
    for pkg_name, info in dependencies.items():
        module = info['module']
        category = info['category']
        required = info['required']
        
        if category not in categories:
            categories[category] = []
        
        try:
            mod = __import__(module)
            status = "✓"
            categories[category].append((status, pkg_name))
        except Exception as e:
            status = "✗"
            error_msg = str(e)
            
            # Manejar el error de pyad en Linux
            if "Must be running Windows" in error_msg:
                status = "⚠"
                categories[category].append((status + " (Windows only)", pkg_name))
            else:
                categories[category].append((status, pkg_name))
                if required:
                    missing_required.append(pkg_name)
                else:
                    missing_optional.append(pkg_name)
    
    # Mostrar por categoría
    for category, items in sorted(categories.items()):
        print(f"\n{category}:")
        for status, pkg in items:
            symbol = "✓" if status.startswith("✓") else ("✗" if status.startswith("✗") else "⚠")
            print(f"  {symbol} {pkg:30}")
    
    print()
    print("=" * 70)
    
    if missing_required:
        print(f"\n⚠️  FALTA INSTALAR (Obligatorias):")
        for pkg in missing_required:
            print(f"   • {pkg}")
        print(f"\nInstalar con: pip3 install {' '.join(missing_required)}")
        return False
    elif missing_optional:
        print(f"\n⚠️  Opcional (no instalado):")
        for pkg in missing_optional:
            print(f"   • {pkg}")
    else:
        print("\n✓ ¡TODAS LAS DEPENDENCIAS INSTALADAS!")
    
    print("=" * 70)
    return True

if __name__ == '__main__':
    success = check_dependencies()
    sys.exit(0 if success else 1)
