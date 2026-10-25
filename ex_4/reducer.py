import sys
import math
max_r=-float("inf")
for line in sys.stdin:
    line=line.strip()
    id,r=line.split()
    r=float(r)
    if r>max_r:
        max_r=r
uniq=(2**max_r)/0.7735
print(f"Max trailing zeros (R): {max_r}")
print(f"Unique values in the stream is: {round(uniq)}")
