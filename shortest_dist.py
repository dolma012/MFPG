import numpy as np
import sys

def find_min_edge_cost(self, source_lst, mfp_edge_cost, queue):
    '''
    min_edge initialzed to None, min_cost initialized to maxsize, iterate through the source nodes
    and check if it forms a valid edge with any node in the queue. If it does then track the cost
    
    '''
    
    min_edge = None
    min_cost = sys.maxsize
    for edge in set(source_lst):
        for e in queue:
            if e!= edge and (edge,e) in mfp_edge_cost:
                if min_cost > mfp_edge_cost[(edge,e)]:
                    min_cost = mfp_edge_cost[(edge,e)]
                    min_edge = (edge,e)
    return min_edge


# dummy node expand to all mfp-nodes that can expand
def compute_shortest_path(self, mfp_edge_cost, mfp_nodes, e_matrix, edge_map, destination_traces, source_traces):
    appended_graph ={}
    dist={}
    prev ={}
    source_lst = []
    queue_lst = []
    for node in mfp_nodes:
        
        dist[node] = sys.maxsize
        prev[node] = None     
        queue_lst.append(node)
    dist["PH"] = 0
    for source in source_traces:
        appended_graph[("PH",source)] = 0
        dist[source] = 0
        source_lst.append(source)
    for edge in mfp_edge_cost:
        appended_graph[edge]=mfp_edge_cost[edge]
    final_trace = None
    while len(queue_lst):
            
        edge=find_min_edge_cost(source_lst, mfp_edge_cost, queue_lst)
        print(edge)
        # get indices where energy is not 0 and then find if the destination is within the trace
        if edge is None or edge[1] in destination_traces:
            if edge is None:
                break
            final_trace=edge[1]
            if dist[final_trace] >= sys.maxsize:
                dist[final_trace] =  mfp_edge_cost[edge]
                prev[edge[1]] = edge[0] 
            break
                                
        else:
        # edge_map maps to indices in the e_matrix columns
            queue_lst.remove(edge[1])
            combo = [(edge[1],check) for check in queue_lst if (edge[1], check) in appended_graph]
            for check_edge in combo:
                if dist[check_edge[0]] >= sys.maxsize:
                    alt =  mfp_edge_cost[check_edge]
                else:
                    alt = dist[check_edge[0]] + mfp_edge_cost[check_edge]
                if alt < dist[check_edge[1]]:
                    dist[check_edge[1]] = alt
                    prev[check_edge[1]] = check_edge[0] 
                    final_trace = check_edge[1]
    if final_trace is None:
        print("No such trace exist")
        return None, None
    shortest_path_cost = dist[final_trace]
    shortest_path = [final_trace]
    while final_trace is not None:
        final_trace = prev[final_trace]
        # print("prev", prev[final_trace])
        if final_trace is not None:
            shortest_path.append(final_trace)
            shortest_path_cost+= dist[final_trace]
    return shortest_path[::-1], shortest_path_cost   