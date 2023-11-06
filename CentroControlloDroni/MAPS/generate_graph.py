import random
import json
import sys

def generate_graph(num_node, min_weight, max_weight, name, max_edge):
    graph = {}
    for i in range(num_node):
        edges = []
        edges_already_connected = []
        for _ in range(random.randint(2,max_edge)):
            
            j = random.randint(0,num_node-1)
            if j == num_node:
                print('error')
            while j == i or j in edges_already_connected:
                j = random.randint(0,num_node-1)
                
            weight = random.randint(min_weight, max_weight)
            edges.append([j, weight])
            edges_already_connected.append(j)
        graph[str(i)] = edges
        
    with open(name + ".json", "w") as outfile:
        json.dump(graph, outfile)
    
    return graph
        
def is_connected(graph: dict):    
    visited = set()
    queue = [list(graph.keys())[0]]
    
    
    while queue:
        current = queue.pop(0)
        visited.add(current)
        #print(graph[str(current)][0])
        for neighbor in graph[str(current)]:
            if neighbor[0] not in visited:
                queue.append(neighbor[0])
    print(len(visited))
    if len(visited)-1 == len(graph):
        return True
    else:
        return False

if __name__ == '__main__':
    name: str = sys.argv[1]
    num_node: int = int(sys.argv[2])
    min_weight: int = int(sys.argv[3])
    max_weight: int = int(sys.argv[4])
    max_edge: int = int(sys.argv[5])
    connected = False
    while not connected:
        graph: dict = generate_graph(num_node,min_weight, max_weight, name, max_edge)
        connected = is_connected(graph)