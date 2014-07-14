import ftplib
import socket
import sys

ftp=ftplib.FTP('127.0.0.1')
try:
  user=sys.argv[1]
  passwd=sys.argv[2]
  ftp.login(user,passwd)
  hand=open('aa.txt','a+')
  hand.write(user+":"+passwd+"\n")
except ftplib.error_perm:
  print "passwd is wrong"
