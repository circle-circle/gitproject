sum1 =0 
def fabi(n): 
   if n == 0:
      return 1
   else:
      return fabi(n-1)*n   
for i in range(1,21):
    sum1 += fabi(i)
print sum1

s =0
l =range(1,21)

def op(x):
    r=1
    for i in range(1,x+1):
        r*=i
    return r
s = sum(map(op,l))
print s

#0!=1
#1!=0!*1=1*1
#2!=1!*2=1*1*2
#3!=2!*3=1*2*#
 
