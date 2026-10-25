import sys

for line in sys.stdin:
    line=line.strip().lower()
    words=line.split()
    for i in range(len(words)-1):
        bigram=",".join(words[i:i+2])
        print(f"{bigram} 1")