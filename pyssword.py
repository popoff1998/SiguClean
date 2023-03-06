import os
import sys
import termios
import fcntl
import getpass

def getch():
  fd = sys.stdin.fileno()

  oldterm = termios.tcgetattr(fd)
  newattr = termios.tcgetattr(fd)
  newattr[3] = newattr[3] & ~termios.ICANON & ~termios.ECHO
  termios.tcsetattr(fd, termios.TCSANOW, newattr)

  oldflags = fcntl.fcntl(fd, fcntl.F_GETFL)
  fcntl.fcntl(fd, fcntl.F_SETFL, oldflags | os.O_NONBLOCK)

  try:
    while 1:
      try:
        c = sys.stdin.read(1)
        break
      except IOError: pass
  finally:
    termios.tcsetattr(fd, termios.TCSAFLUSH, oldterm)
    fcntl.fcntl(fd, fcntl.F_SETFL, oldflags)
  return c

def pyssword(prompt='Password: '):
  '''
      Pregunta por una password enmascarando la salida.
      Returns:
          el valor introducido.
  '''

  if sys.stdin is not sys.__stdin__:
      pwd = getpass.getpass(prompt)
      return pwd
  else:
      pwd = ""
      sys.stdout.write(prompt)
      sys.stdout.flush()
      while True:
          key = ord(getch())
          if key == 10: #Return Key
              sys.stdout.write('\n')
              #print "PASS=",pwd," TIPO: ",type(pwd)
              return pwd
              break
          if key == 127: #Backspace key
              if len(pwd) > 0:
                  # Erases previous character.
                  sys.stdout.write('\b' + ' ' + '\b')
                  sys.stdout.flush()
                  pwd = pwd[:-1]
          else:
              # Masks user input.
              char = chr(key)
              sys.stdout.write('*')
              sys.stdout.flush()
              pwd = pwd + char
