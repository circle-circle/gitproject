#us=open("user.txt","w+")
ps=open('passwd.txt','w+')

s='abcdefghigjkmnopqrstuvwxyz1234567890'
base=len(s)
end=len(s)**4

for i in range(0,end):
    n = i
    ch0=s[n%base]
    n=n/base
    ch1=s[n%base]
    n=n/base
    ch2=s[n%base]
    n=n/base
    ch3=s[n%base]
    ps.write(ch3+ch2+ch1+ch0+'\r\n')
ps.close()
