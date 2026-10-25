bloomsize=10000
import sys
dupe=[]
real=[]
for line in sys.stdin:
    line=line.strip()
    id,value=line.split()
    if value=="UNIQUE":
        real.append(id)
    else:
        dupe.append(id)
print(f"Unique:{",".join(real)}")
print(f"duplicates:{",".join(dupe)}")

