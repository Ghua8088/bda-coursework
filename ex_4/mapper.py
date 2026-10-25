import hashlib
import sys 

for line in sys.stdin:
    line=line.strip()
    id=line.split(",")[0]
    h=int(hashlib.sha256(id.encode()).hexdigest(),16)
    bin_h=bin(h)[2:]
    c=0
    for i in bin_h[::-1]:
        if i=='0':
            c+=1
        else:
            break
    print(f"{id} {c}")