count = []
for a in range(1,5):
    for b in range(1,5):
        for c in range(1,5):
             if a != b and b != c and a != c:
                d = 100*a + 10*b + c
                count.append(d)
print count
print "there is %d result" % len(count) 
