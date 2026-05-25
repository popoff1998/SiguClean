# Solución: Error "AttributeError: 'NoneType' object has no attribute 'cursor'"

## Problema
Cuando ejecutas el comando `stats` en la sesión interactiva de Siguclean, obtienes el siguiente error:

```
AttributeError: 'NoneType' object has no attribute 'cursor'
```

Esto ocurre en la línea:
```python
cursor = config.oracleCon.cursor()
```

## Causa
La variable `config.oracleCon` es `None`, lo que significa que **la conexión a la base de datos Oracle no se ha establecido**.

Esto sucede cuando se ejecuta:
```bash
python3 siguclean.py -i
```

Sin pasar credenciales de Oracle, la inicialización de la conexión no se ejecuta correctamente.

## Solución

### Opción 1: Pasar credenciales por línea de comandos (Recomendado)
```bash
python3 siguclean.py -i --sigu-password tu_contraseña_sigu
```

Ejemplo:
```bash
python3 siguclean.py -i --sigu-password micontraseña123
```

### Opción 2: Proporcionar las credenciales al iniciar
La aplicación te pedirá las credenciales interactivamente:

```bash
python3 siguclean.py -i
```

Luego deberá aceptar la solicitud de contraseña que aparecerá.

## Requisitos para que funcione

### 1. Credenciales de Oracle
- **Usuario**: `sigu` (por defecto)
- **Contraseña**: Tu contraseña de usuario `sigu` en Oracle
- **Servidor**: `ora-av10g.bbdd.uco.es/av10g` (configurable en `config.py`)

### 2. Acceso a la base de datos
- Debes tener acceso a la base de datos Oracle especificada
- Las conexiones de red deben estar activas
- El servidor Oracle debe estar disponible

### 3. Dependencias instaladas
- `cx-Oracle` debe estar instalado: `pip3 install cx-Oracle`
- Las librerías de Oracle Client deben estar disponibles en el sistema

## Verificación de la solución

Después de ejecutar con credenciales correctas, verifica que la conexión se ha establecido:

```bash
(Cmd) help stats
(Cmd) stats
```

Si ves una tabla con estadísticas, ¡la conexión está funcionando!

## Más información

- Ver [DEPENDENCIAS.md](DEPENDENCIAS.md) para instrucciones de instalación
- Ver [config.py](config.py) para configuración del servidor Oracle
- Usar `help` en la sesión interactiva para ver comandos disponibles

## Comandos útiles en la sesión interactiva

```bash
(Cmd) help              # Ver todos los comandos disponibles
(Cmd) stats             # Ver estadísticas de almacenamiento (requiere Oracle)
(Cmd) users -d <fecha>  # Listar usuarios desde una fecha
(Cmd) help <comando>    # Ayuda sobre un comando específico
(Cmd) exit              # Salir
```
