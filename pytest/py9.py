import math
#import pdb
result = []
leap = 1
#pdb.set_trace()
for i in  xrange(100,20000000):
    m = int(math.sqrt(i))
    for j in range(2,m+1):
        if i%j ==0:
           leap =0
           break
    if leap == 1:
       result.append(i)
    leap = 1
print "the sushu is %s" %result
print "sum is %d"  %len(result)      
         

     
