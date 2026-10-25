import sys
for line in sys.stdin:
    line=line.strip()
    month,min,max=line.split(",")
    print(f"{month} {min}")
