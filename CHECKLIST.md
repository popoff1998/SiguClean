# CHECKLIST DE VERIFICACIÓN - SIGUCLEAN 3.9

## ✅ ANTES DE USAR

### Sistema Operativo
- [x] Rocky Linux 9.6 instalado
- [x] Acceso root/sudo disponible
- [x] Conectividad a red verificada

### Python
- [x] Python 3.9.21 instalado
- [x] Todos los archivos compilados sin errores
- [x] Migración 2.7 → 3.9 completada

### Dependencias Python
- [x] python-dateutil 2.8.1
- [x] tenacity 9.1.2
- [x] cx-Oracle 8.3.0
- [x] python-ldap 3.3.0
- [x] pyad 0.6.0
- [x] progressbar33 2.4
- [x] texttable 1.7.0

### Oracle Client
- [x] Oracle Instant Client 19.x instalado
- [x] libclntsh.so disponible
- [x] ldconfig configurado
- [x] LD_LIBRARY_PATH configurado
- [x] /etc/ld.so.conf.d/oracle.conf creado

### Código Mejorado
- [x] checkOracleConnection() con mejor error handling
- [x] Verificación de conexión antes de usar cursor
- [x] check_environment() llamado en modo interactivo

---

## 📋 VERIFICAR ANTES DE EJECUTAR

```bash
# 1. Sistema
python3 /opt/siguclean/verify_rocky.py
# Debe mostrar: ✓ en todas las secciones

# 2. Dependencias
python3 /opt/siguclean/check_dependencies.py
# Debe mostrar: ✓ todas las dependencias

# 3. Oracle (requiere contraseña)
python3 /opt/siguclean/test_quick.py
# Debe mostrar: ✓ Conexión exitosa
```

---

## 🚀 EJECUTAR SIGUCLEAN

### Opción 1: Script Rápido (RECOMENDADO)
```bash
bash /opt/siguclean/run_siguclean.sh tu_contraseña
```

### Opción 2: Directamente
```bash
python3 /opt/siguclean/siguclean.py -i --sigu-password tu_contraseña
```

### Opción 3: Modo Batch
```bash
bash /opt/siguclean/run_siguclean.sh tu_contraseña batch /ruta/a/procesar
```

---

## 🔍 VERIFICAR FUNCIONAMIENTO

### En modo interactivo:
```
(Cmd) help              # Ver todos los comandos
(Cmd) stats             # Debe conectar a Oracle
(Cmd) add /home         # Agregar ruta
(Cmd) process           # Procesar
(Cmd) quit              # Salir
```

### Errores esperados:
```
✓ ERROR: No hay conexión a Oracle
  → Credencial incorrecta, verificar con Oracle Admin

✓ WARNING: Permission denied /ruta
  → Usuario no tiene permisos, usar sudo o cambiar user

✓ ERROR: Mount point /mnt/share not found
  → Ruta no existe o no está montada
```

---

## 🛠️ TROUBLESHOOTING RÁPIDO

| Problema | Causa | Solución |
|----------|-------|----------|
| DPI-1047 | libclntsh no encontrada | `sudo ldconfig -v` |
| AttributeError cursor | Oracle no conectó | Verificar contraseña |
| ORA-12514 | Servicio Oracle no disponible | `ping ora-av10g.bbdd.uco.es` |
| Permission denied | Sin permisos en ruta | `sudo bash run_siguclean.sh ...` |
| ModuleNotFoundError | Dependencia faltante | `pip3 install -r requirements.txt` |

---

## 📊 INFORMACIÓN DEL SISTEMA

```
OS:           Rocky Linux 9.6
Python:       3.9.21
cx-Oracle:    8.3.0
Oracle Server: ora-av10g.bbdd.uco.es/av10g
Oracle User:   sigu
Oracle Client: /opt/oracle/instantclient_*
```

---

## 📞 DOCUMENTACIÓN

- **ESTADO.md** - Estado completo y troubleshooting
- **INSTALAR_RAPIDO.md** - Guía rápida
- **ROCKY_LINUX_9_ORACLE.md** - Guía Rocky Linux detallada
- **requirements.txt** - Todas las dependencias

---

## ✨ LISTO PARA USAR

```bash
bash /opt/siguclean/run_siguclean.sh contraseña
```

¡La aplicación está completamente funcional en Python 3.9 con Rocky Linux 9.3!
