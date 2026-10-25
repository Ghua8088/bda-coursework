import sys
month=None
min=float('inf')
for line in sys.stdin:
    line=line.strip()
    c_month,c_min=line.split()
    c_min=float(c_min)
    if min>c_min:
        min=c_min
        month=c_month
print(f"{month} {min}")
