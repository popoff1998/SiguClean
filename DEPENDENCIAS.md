# Dependencias de Siguclean

## Instalación rápida

### Opción 1: Usar el script de instalación (recomendado)
```bash
cd /opt/siguclean
chmod +x install_dependencies.sh
./install_dependencies.sh
```

### Opción 2: Instalación manual
```bash
cd /opt/siguclean
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

### Opción 3: Instalación individual
```bash
python3 -m pip install python-dateutil>=2.8.0
python3 -m pip install tenacity>=9.0.0
python3 -m pip install cx-Oracle>=8.0.0
python3 -m pip install python-ldap>=3.3.0
python3 -m pip install pyad>=0.5.20
python3 -m pip install progressbar33>=2.4.0
python3 -m pip install texttable>=1.6.0
```

## Total: 7 dependencias externas

### Obligatorias (Core)

#### 1. **python-dateutil** (≥ 2.8.0)
- **Uso**: Parsing y manipulación de fechas
- **Importado en**: `sc_funcs.py`, `siguclean.py`
- **Instalación**: `pip3 install python-dateutil`
- **Nota**: Suele estar preinstalado en muchos sistemas

#### 2. **tenacity** (≥ 9.0.0)
- **Uso**: Decoradores para reintentos automáticos
- **Importado en**: `sc_funcs.py`
- **Instalación**: `pip3 install tenacity`
- **Alternativa en Python estándar**: `retrying` (más antiguo)

### Opcionales pero recomendadas

#### 3. **cx-Oracle** (≥ 8.0.0)
- **Uso**: Conexión a base de datos Oracle
- **Importado en**: `sc_classes.py`, `sigudb.py`, `sc_funcs.py`
- **Instalación**: `pip3 install cx-Oracle`
- **Dependencias del sistema**: 
  - Se requiere Oracle Client bibliotecas instaladas
  - En Ubuntu/Debian: `sudo apt-get install libaio1`
  - En RedHat/CentOS: `sudo yum install libaio`

#### 4. **python-ldap** (≥ 3.3.0)
- **Uso**: Conexión a servidores LDAP
- **Importado en**: `sc_funcs.py`, `sigudb.py`, `sc_classes.py`
- **Instalación**: `pip3 install python-ldap`
- **Dependencias del sistema (Linux)**:
  ```bash
  # Ubuntu/Debian
  sudo apt-get install libldap2-dev libsasl2-dev
  
  # RedHat/CentOS
  sudo yum install openldap-devel cyrus-sasl-devel
  
  # macOS
  brew install openldap cyrus-sasl
  ```

#### 5. **pyad** (≥ 0.5.20)
- **Uso**: Interacción con Active Directory
- **Importado en**: `sigudb.py`
- **Instalación**: `pip3 install pyad`
- **Nota**: Requiere acceso a un servidor AD

### Interfaz de Usuario

#### 6. **progressbar33** (≥ 2.4.0)
- **Uso**: Barra de progreso en terminal
- **Importado en**: `siguclean.py`, `sc_classes.py`
- **Instalación**: `pip3 install progressbar33`
- **Alternativa**: `tqdm` (más moderno)

#### 7. **texttable** (≥ 1.6.0)
- **Uso**: Formateo de tablas en terminal
- **Importado en**: `sc_funcs.py`, `sc_shell.py`
- **Instalación**: `pip3 install texttable`
- **Nota**: Usado para mostrar datos en formato de tabla tabulada

## Módulos estándar (sin instalación requerida)

La aplicación también usa módulos estándar de Python:
- `sys`, `os` - Interacción con el sistema
- `subprocess` - Ejecución de comandos
- `datetime`, `dateutil` - Manejo de fechas
- `re` - Expresiones regulares
## Módulos estándar (sin instalación requerida)

La aplicación también usa módulos estándar de Python:
- `sys`, `os` - Interacción con el sistema
- `subprocess` - Ejecución de comandos
- `datetime` - Manejo de fechas
- `re` - Expresiones regulares
- `argparse` - Parsing de argumentos
- `pickle` - Serialización
- `tarfile` - Operaciones con archivos TAR
- `shutil` - Operaciones de archivo de alto nivel
- `enum` - Enumeraciones
- `contextlib` - Gestores de contexto
- `collections` - Tipos de datos
- `readline`, `cmd`, `ast` - Interacción de terminal y análisis
- `glob` - Búsqueda de archivos con patrones
- `itertools` - Herramientas de iteración
- `gc` - Garbage collector
- `getpass`, `termios`, `fcntl` - Operaciones de terminal

## Resolución de problemas

### Error: "ModuleNotFoundError: No module named 'cx_Oracle'"
**Solución**: `pip3 install cx-Oracle` o instala las dependencias de Oracle Client

### Error: "ModuleNotFoundError: No module named 'ldap'"
**Solución**: 
```bash
# Linux
sudo apt-get install libldap2-dev libsasl2-dev
pip3 install python-ldap
```

### Error: "ModuleNotFoundError: No module named 'pyad'"
**Solución**: `pip3 install pyad` (requiere conexión a AD)

### Error: "ModuleNotFoundError: No module named 'progressbar'"
**Solución**: `pip3 install progressbar33` (nota: el módulo se llama `progressbar` pero el paquete es `progressbar33`)

### Error: "ModuleNotFoundError: No module named 'texttable'"
**Solución**: `pip3 install texttable`

## Verificación de instalación

```bash
# Verificar todas las dependencias
python3 << 'EOF'
dependencies = {
    'dateutil': 'python-dateutil',
    'tenacity': 'tenacity',
    'cx_Oracle': 'cx-Oracle (opcional)',
    'ldap': 'python-ldap (opcional)',
    'pyad': 'pyad (opcional)',
    'progressbar': 'progressbar33 (opcional)'
}

for module, package in dependencies.items():
    try:
        __import__(module)
        print(f'✓ {package}')
    except ImportError:
        print(f'✗ {package}')
EOF
```

## Actualizar todas las dependencias

```bash
# Actualizar todos los paquetes en requirements.txt
pip3 install --upgrade -r requirements.txt

# Crear un archivo con versiones actuales
pip3 freeze > requirements-lock.txt
```

## Desarrollo

Si quieres contribuir o modificar el código:

```bash
# Instalar herramientas de desarrollo
pip3 install black flake8 pytest

# Formatear código
black *.py

# Verificar estilo
flake8 *.py

# Ejecutar tests (si existen)
pytest
```

## Notas finales

- **Python 3.9 mínimo**: La aplicación requiere Python 3.9 o superior
- **Sistema operativo**: Funciona en Linux (primordialmente), macOS y Windows
- **Permisos**: Algunos módulos (ldap, cx_Oracle) pueden requerir compilación, necesitando herramientas de desarrollo
- **Virtual Environment**: Se recomienda usar `venv` o `virtualenv` para aislar las dependencias
