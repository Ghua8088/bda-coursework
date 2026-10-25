import sys 
BETA=0.8
N=10
c_page=None
links=[]
rank_sum=0.0
for line in sys.stdin:
    line=line.strip()
    parts=line.split()
    page=parts[0]
    type=parts[1]
    values=parts[2:]
    if c_page!=page:
        if c_page is not None:
            adj=" ".join(links)
            print(f"{c_page}  {(BETA*rank_sum)+((1-BETA)/N)} {adj}")
        c_page=page
        rank_sum=0.0
        links=[]
        if type=="STRUCT":
            links=values
        else:
            values=float(values[0])
            rank_sum=values
    else:
        if type=="STRUCT":
            links.extend(values)
        else:
            values=float(values[0])
            rank_sum+=values
if c_page is not None:
    adj = " ".join(links)
    new_rank = (BETA * rank_sum) + ((1 - BETA) / N)
    print(f"{c_page} {new_rank} {adj}".strip())
    
