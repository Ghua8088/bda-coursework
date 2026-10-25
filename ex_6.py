import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from networkx.algorithms.community import modularity
G = nx.relaxed_caveman_graph(l=3, k=5, p=0.15, seed=42)
plt.figure(figsize=(7, 5))
pos = nx.spring_layout(G, seed=42)
def edge_to_remove(graph):
    G_dict = nx.edge_betweenness_centrality(graph)
    sorted_edges = sorted(G_dict.items(), key=lambda item: item[1], reverse=True)
    return sorted_edges[0][0]
def automated_girvan_newman(graph):
    original_graph = graph.copy()
    graph_working = graph.copy()
    best_modularity = -1
    best_communities = []
    while graph_working.number_of_edges() > 0:
        edge = edge_to_remove(graph_working)
        graph_working.remove_edge(edge[0], edge[1])
        current_communities = tuple(nx.connected_components(graph_working))
        if len(current_communities) > 1:
            current_modularity = modularity(original_graph, current_communities)
            if current_modularity > best_modularity:
                best_modularity = current_modularity
                best_communities = current_communities
               
    print(f" Highest Modularity Peak Found: {best_modularity:.4f}")
    return best_communities
best_factions = automated_girvan_newman(G)
node_groups = [list(c) for c in best_factions]
num_communities = len(node_groups)


print(f"Algorithm auto-detected exactly {num_communities} communities.")
for i, community in enumerate(node_groups, 1):
    print(f"Community {i}: {community}")
cmap = plt.cm.get_cmap('rainbow', num_communities)
colors = [mcolors.to_hex(cmap(i)) for i in range(num_communities)]


color_map = []
for node in G:
    for idx, group in enumerate(node_groups):
        if node in group:
            color_map.append(colors[idx])
            break
plt.figure(figsize=(7, 5))
nx.draw(G, pos, node_color=color_map, with_labels=True, node_size=500, edge_color='#bdc3c7')
plt.title(f"Automated Selection: {num_communities} Clusters Found")
plt.show()



