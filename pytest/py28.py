def age(n):
    if n ==1:
       return 10
    else:
       return age(n-1)+2
print age(5)

hw=[]
for x in xrange(10000,99999):
   x= str(x)
   for i in range(len(x)):
       if  x[i] != x[-i-1]:
           break
   else:
       hw.append(x)
       print "huinwen %s,count %d" %(hw,len(hw))
