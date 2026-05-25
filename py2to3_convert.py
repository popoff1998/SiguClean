#!/usr/bin/env python3
import re
import sys
import os

def convert_file(filepath):
    """Convierte un archivo Python 2 a Python 3"""
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    
    original_content = content
    
    # 1. Cambiar print statements sin paréntesis a funciones
    # Pattern: print "string", variable, etc. (but not print(...) which is already correct)
    # Manejo de print simple
    content = re.sub(
        r'\bprint\s+"([^"]*)"(?=\s|$|\n)',
        r'print("\1")',
        content
    )
    
    # Cambiar print con múltiples argumentos separados por comas
    # print "algo", var, "otro" -> print("algo", var, "otro")
    lines = content.split('\n')
    new_lines = []
    
    for line in lines:
        # Skip lines that already have print(...)
        if 'print(' in line and not re.search(r'print\s+"', line):
            new_lines.append(line)
            continue
            
        # Detectar print statements old-style
        match = re.match(r'^(\s*)print\s+(.+)$', line)
        if match:
            indent = match.group(1)
            content_after_print = match.group(2)
            
            # Si ya tiene paréntesis, skip
            if content_after_print.startswith('('):
                new_lines.append(line)
            else:
                # Convertir print x, y, z a print(x, y, z)
                new_line = f"{indent}print({content_after_print})"
                new_lines.append(new_line)
        else:
            new_lines.append(line)
    
    content = '\n'.join(new_lines)
    
    if content != original_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python3 py2to3_convert.py <archivo_o_directorio>")
        sys.exit(1)
    
    path = sys.argv[1]
    
    if os.path.isfile(path):
        if convert_file(path):
            print(f"Convertido: {path}")
        else:
            print(f"Sin cambios: {path}")
    elif os.path.isdir(path):
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.py'):
                    filepath = os.path.join(root, file)
                    if convert_file(filepath):
                        print(f"Convertido: {filepath}")
