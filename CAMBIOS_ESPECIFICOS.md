# Cambios específicos de migración Python 2.7 → 3.9

## Lista de cambios por archivo

### siguclean.py
- **Línea 1**: Shebang cambió de `#!/usr/bin/python2.7` a `#!/usr/bin/python3`
- **Línea 138**: `print __version__` → `print(__version__)`
- **Línea 166**: `except BaseException, e:` → `except BaseException as e:`

### sc_funcs.py
- **Línea 1**: Shebang actualizado
- **Línea 31**: `print "TRACE: ..."` → `print("TRACE: ...")`
- **Línea 74**: `raw_input(...)` → `input(...)`
- **Línea 91-100**: Función `_pprint` - `print arg,` → `print(arg, end=' ')`
- **Línea 291**: `raw_input(...)` → `input(...)`
- **Línea 308-315**: Print statements múltiples convertidos
- **Línea 317**: `raw_input(...)` → `input(...)`
- **Línea 346**: `raw_input(...)` → `input(...)`
- **Línea 494, 510**: Print statements de error convertidos
- **Línea 982-1009**: Print statements de escritura de archivos convertidos

### sc_classes.py
- **Línea 1**: Shebang actualizado
- **Línea 27-28**: Enum actualizado de sintaxis antigua a nueva
  ```python
  # Antes
  state = Enum('NA', 'ARCHIVED', 'DELETED', ...)
  # Después
  state = Enum('state', 'NA ARCHIVED DELETED ...')
  ```
- **Línea 200, 203, 206, 209, 212**: Print statements con comas convertidos
- **Línea 1125**: `print "__DEL__..."` → `print("__DEL__...")`

### sc_shell.py
- **Línea 1**: Shebang actualizado
- **Líneas 58, 90, 92, 99, 101, etc.**: Todos los print statements convertidos a funciones
- Múltiples instancias de `print "string",` convertidas a `print("string",`

### config.py
- **Línea 131**: Enum actualizado
  ```python
  # Antes
  reason = Enum('NOTINLDAP', 'NOMANDATORY', "FAILARCHIVE", ...)
  # Después
  reason = Enum('reason', 'NOTINLDAP NOMANDATORY FAILARCHIVE ...')
  ```
- **Línea 30**: Print statements en función de debug convertidos

### sigudb.py
- **Línea 33-36**: Print statements y `raw_input()` convertidos

### sc_log.py
- **Línea 1**: Shebang actualizado

### pyssword.py
- Sin cambios significativos (comentarios ajustados)

## Resumen de patrones de migración

### 1. Print statements
```python
# Python 2
print "message"
print "a", b, "c"
print variable,  # sin newline

# Python 3
print("message")
print("a", b, "c")
print(variable, end=' ')  # sin newline
```

### 2. Input
```python
# Python 2
text = raw_input("prompt: ")

# Python 3
text = input("prompt: ")
```

### 3. Except statements
```python
# Python 2
except Exception, e:

# Python 3
except Exception as e:
```

### 4. Enum
```python
# Python 2 (functional API antigua)
MyEnum = Enum('Value1', 'Value2', 'Value3')

# Python 3 (functional API nueva)
MyEnum = Enum('MyEnum', 'Value1 Value2 Value3')
```

## Comprobación de cambios

Para verificar que los cambios se aplicaron correctamente:
```bash
# Validar sintaxis
python3 -m py_compile *.py

# Ejecutar script de verificación
python3 verify_python3_migration.py

# Ver diferencias
diff -u sc_funcs.py.OLD sc_funcs.py  # (si existe el archivo .OLD)
```

## Notas
- No se modificó la lógica de la aplicación, solo la sintaxis
- El archivo `sc_funcs.py.OLD` conserva la versión original en Python 2.7
- Todos los comentarios en español han sido preservados
- La aplicación mantiene la misma funcionalidad en Python 3.9
