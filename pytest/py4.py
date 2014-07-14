x = int(raw_input("please input num1:"))
y = int(raw_input("please input num2:"))
z = int(raw_input("please input num3:"))

if x < y :
   if x < z:
       if y < z:
          print x,y,z
       else:
          print x,z,y
   else: # y>x>z
      print z,x,y
else: #x>y
   if y >z:
      print z,y,x
   else: # y<z x>y
      if z >x:
         print y,x,z
      else:
         print y,z,x
