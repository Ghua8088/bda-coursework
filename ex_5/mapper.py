import sys

for line in sys.stdin:
    line=line.strip()
    parts=line.split()
    source=parts[0]
    rank=float(parts[1])
    adj=parts[2:]
    print(f"{source} STRUCT {' '.join(adj)}")
    if len(adj)>0:
        contribution=rank/len(adj)
        for node in adj:
            print(f"{node} VALUE {contribution}")