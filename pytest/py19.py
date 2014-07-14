#from  sys import stdout
n=int(raw_input("input num:"))
print "%d=" %n,   
for i in xrange(2,n+1):
    while n != i:
          if n % i ==0:
             #stdout.write(str(i))
             #stdout.write("*")
             print "%d*" %i,
             n=n/i
          else:
             break

print "%d" %n
