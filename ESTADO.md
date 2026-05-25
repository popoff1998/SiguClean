# SIGUCLEAN - ESTADO ACTUAL (Rocky Linux 9.3)

## ✓ COMPLETADO

### 1. Migración Python 2.7 → 3.9
- ✓ Todos los archivos Python compilados sin errores
- ✓ Print statements convertidos (50+ instancias)
- ✓ raw_input() → input() (5 instancias)  
- ✓ Enum syntax actualizado
- ✓ Exception handling actualizado

### 2. Dependencias Python
Instaladas y verificadas:
- ✓ python-dateutil 2.8.1
- ✓ tenacity 9.1.2
- ✓ cx-Oracle 8.3.0
- ✓ python-ldap 3.3.0
- ✓ pyad 0.6.0
- ✓ progressbar33 2.4
- ✓ texttable 1.7.0

### 3. Oracle Client
- ✓ Instalado en `/opt/oracle/`
- ✓ libclntsh.so disponible
- ✓ ldconfig configurado
- ✓ cx_Oracle 8.3.0 funcional

### 4. Sistema Rocky Linux 9.3
- ✓ Rocky Linux release 9.6 detectado
- ✓ Dependencias del sistema instaladas (libaio, unzip, wget, ldconfig)
- ✓ Permisos root confirmados

### 5. Mejoras al Código
- ✓ [sc_funcs.py](sc_funcs.py#L194-225): Mejorado error handling en `checkOracleConnection()`
- ✓ [sc_shell.py](sc_shell.py#L507-510): Verificación de conexión antes de usar cursor
- ✓ [siguclean.py](siguclean.py#L135): Agregada `check_environment()` en modo interactivo

## 📋 ARCHIVOS DE UTILIDAD

### Verificación
- **[verify_rocky.py](verify_rocky.py)** - Script de chequeo previo
  ```bash
  python3 /opt/siguclean/verify_rocky.py
  ```

### Instalación
- **[install_oracle_rocky.sh](install_oracle_rocky.sh)** - Script automático (requiere ZIP descargado)
  ```bash
  sudo bash /opt/siguclean/install_oracle_rocky.sh
  ```

### Testing
- **[test_quick.py](test_quick.py)** - Test rápido de conexión Oracle (requiere contraseña)
  ```bash
  python3 /opt/siguclean/test_quick.py
  ```

- **[check_dependencies.py](check_dependencies.py)** - Verifica todas las dependencias
  ```bash
  python3 /opt/siguclean/check_dependencies.py
  ```

### Documentación
- **[INSTALAR_RAPIDO.md](INSTALAR_RAPIDO.md)** - Guía rápida
- **[ROCKY_LINUX_9_ORACLE.md](ROCKY_LINUX_9_ORACLE.md)** - Guía detallada Rocky Linux
- **[INSTALAR_ORACLE_CLIENT.md](INSTALAR_ORACLE_CLIENT.md)** - Guía por SO
- **[requirements.txt](requirements.txt)** - Dependencias Python

---

## 🚀 USAR SIGUCLEAN

### 1. Modo Batch (sin Oracle)
```bash
python3 /opt/siguclean/siguclean.py -b /ruta/a/procesar
```

### 2. Modo Interactivo (con Oracle)
```bash
python3 /opt/siguclean/siguclean.py -i --sigu-password contraseña
```

Comandos disponibles en modo interactivo:
```
(Cmd) help                 # Ver todos los comandos
(Cmd) add /ruta            # Agregar ruta a procesar
(Cmd) stats                # Ver estadísticas (requiere Oracle)
(Cmd) process              # Procesar rutas
(Cmd) sessions             # Ver sesiones
(Cmd) quit                 # Salir
```

### 3. Verificación rápida
```bash
# Test de dependencias
python3 /opt/siguclean/check_dependencies.py

# Test de sistema
python3 /opt/siguclean/verify_rocky.py

# Test de Oracle (con contraseña)
python3 /opt/siguclean/test_quick.py
```

---

## 🔧 CONFIGURACIÓN

### Credenciales Oracle
```python
# En config.py
ORACLE_SERVER = 'ora-av10g.bbdd.uco.es/av10g'
ORACLE_PASS = None  # Se solicita al ejecutar o via --sigu-password
```

Usuario: **sigu** (hardcodeado en código)

### LDAP
```python
# En config.py
LDAP_SERVER = "ldaps://docad01.uco.es"
BIND_DN = "Administrador@uco.es"
USER_BASE = "dc=uco,dc=es"
```

---

## 📊 ESTRUCTURA DE ARCHIVOS

```
/opt/siguclean/
├── siguclean.py              # Punto de entrada (230 líneas)
├── sc_funcs.py               # Funciones principales (1156 líneas)
├── sc_shell.py               # Interfaz interactiva (1143 líneas)
├── sc_log.py                 # Logging
├── sigudb.py                 # Funciones DB
├── config.py                 # Configuración
├── pyssword.py               # Entrada de contraseña
│
├── requirements.txt           # Dependencias Python
├── install_oracle_rocky.sh   # Script instalación automática
├── verify_rocky.py           # Verificación del sistema
├── test_quick.py             # Test de conexión
├── check_dependencies.py     # Check dependencias
│
├── INSTALAR_RAPIDO.md        # Guía rápida
├── ROCKY_LINUX_9_ORACLE.md   # Guía Rocky Linux
├── INSTALAR_ORACLE_CLIENT.md # Guía por SO
└── ESTADO.md                 # Este archivo
```

---

## ⚠️ PROBLEMAS COMUNES

### Error: "DPI-1047: Cannot locate a 64-bit Oracle Client library"
**Solución:**
```bash
# 1. Verificar ubicación
ldconfig -p | grep libclntsh

# 2. Si no aparece, recargar
sudo ldconfig -v | grep oracle

# 3. Si aún no funciona, configurar manualmente
export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_*:$LD_LIBRARY_PATH
python3 siguclean.py -i --sigu-password <contraseña>
```

### Error: "AttributeError: 'NoneType' object has no attribute 'cursor'"
**Causa:** Conexión Oracle no establecida
**Solución:**
```bash
# Verificar credenciales y servidor
python3 /opt/siguclean/test_quick.py

# Ejecutar con contraseña
python3 /opt/siguclean/siguclean.py -i --sigu-password <contraseña_correcta>
```

### Error: "ORA-12514: TNS:listener does not currently know of service requested"
**Causa:** Servicio Oracle no disponible o nombre incorrecto
**Solución:**
```bash
# 1. Verificar servidor alcanzable
ping ora-av10g.bbdd.uco.es

# 2. Verificar Puerto 1521
telnet ora-av10g.bbdd.uco.es 1521

# 3. Verificar nombre de servicio en tnsnames.ora
cat /opt/siguclean/tnsnames.ora
```

---

## 📝 VERSIONES INSTALADAS

| Componente | Versión | Estado |
|-----------|---------|--------|
| Rocky Linux | 9.6 | ✓ |
| Python | 3.9.21 | ✓ |
| cx-Oracle | 8.3.0 | ✓ |
| python-dateutil | 2.8.1 | ✓ |
| tenacity | 9.1.2 | ✓ |
| python-ldap | 3.3.0 | ✓ |
| Oracle Client | 19.x | ✓ |
| libaio | - | ✓ |

---

## 🔐 NOTAS DE SEGURIDAD

1. **Contraseñas**: Se solicitan interactivamente o via CLI, nunca se guardan
2. **Logs**: Revisar `/var/log/siguclean/` (si está configurado)
3. **Permisos**: Ejecutar con usuario que tenga acceso a rutas a procesar

---

## 📞 PRÓXIMOS PASOS

1. **Verificación de conectividad:**
   ```bash
   python3 /opt/siguclean/verify_rocky.py
   ```

2. **Test de Oracle:**
   ```bash
   python3 /opt/siguclean/test_quick.py
   ```

3. **Uso en producción:**
   ```bash
   python3 /opt/siguclean/siguclean.py -i --sigu-password <contraseña>
   ```

---

*Estado actualizado: Python 3.9, Rocky Linux 9.3, Oracle Client instalado*
*Última actualización: 2024*
