import sys

prev=None
c=0
for line in sys.stdin:
    line=line.strip().lower()
    bigram,count=line.split()
    count=int(count)
    if prev==bigram:
        c+=count
    else:
        if prev is not None:
            print(f"{prev} {c}")
        prev=bigram
        c=count
if prev is not None:
    print(f"{prev} {c}")
