import sys
import hashlib
bloomsize=10000
bloomfilter=[0]*bloomsize
def hash1(value):
    return int(hashlib.md5(value.encode()).hexdigest(),16) % bloomsize
def hash2(value):
    return int(hashlib.sha256(value.encode()).hexdigest(),16) % bloomsize
def hash3(value):
    return int(hashlib.sha1(value.encode()).hexdigest(),16) % bloomsize
for line in sys.stdin:
    line=line.strip().lower()
    if not line:
        continue
    userid=line.split(",")[0]
    h1=hash1(userid)
    h2=hash2(userid)
    h3=hash3(userid)
    if bloomfilter[h1]==bloomfilter[h2]==bloomfilter[h3]==1:
        print(f"{userid} DUPLICATE")
    else:
        print(f"{userid} UNIQUE")
    bloomfilter[h1]=bloomfilter[h2]=bloomfilter[h3]=1
    