import sys
prev=None
curr=0
for line in sys.stdin:
    line=line.strip().lower()
    word,count=line.split()
    count=int(count)
    if word==prev:
        curr+=count
    else:
        if prev is not None:
            print(f"{prev} {curr}")
        prev=word
        curr=count
