s = raw_input("input string:")
letters =0
space =0
digit =0
others = 0
for c in s:
    if c.isalpha():
       letters +=1
    if c.isspace():
       space +=1
    if c.isdigit():
       digit +=1
    else:
       others +=1
print "char =%d,space=%d,digit=%d,others=%d" %(letters,space,digit,others)

