
import numpy as np
import sys

def find_min_edge_cost( source_lst, mfp_edge_cost, queue):
    '''
    min_edge initialzed to None, min_cost initialized to maxsize, iterate through the source nodes
    and check if it forms a valid edge with any node in the queue. If it does then track the cost
    '''
    min_edge = None
    min_cost = sys.maxsize
    for edge in set(source_lst):
        #for every path in queue, and and MFP-edge in mfp_edge_cost
    
        for e in queue:
            if e!= edge and (edge,e) in mfp_edge_cost:
                if min_cost > mfp_edge_cost[(edge,e)]:
                    min_cost = mfp_edge_cost[(edge,e)]
                    min_edge = (edge,e)
    #finds the min MFP-edge
    return min_edge

def compute_shortest_path(mfp_edge_cost, mfp_nodes, e_matrix, edge_map, destination_traces, source_traces,source_node):
    adj_matrix = np.zeros((len(mfp_nodes), len(mfp_nodes)))
    adj_matrix_weight = np.zeros((len(mfp_nodes), len(mfp_nodes)))
    dummy_node_adj_matrix_weight = np.zeros(len(mfp_nodes))
    adj_matrix_index={}
    for indx, val in enumerate(mfp_nodes):
        adj_matrix_index[val] = indx
    print(adj_matrix_index)
    for e in mfp_edge_cost:
        if e[0] in source_traces :
            arr_d = dummy_node_adj_matrix_weight
            arr_d[adj_matrix_index[e[0]]] = np.sum(e_matrix[e[0]])
            dummy_node_adj_matrix_weight = arr_d
        col_indx = adj_matrix_index[e[1]]
        row_indx = adj_matrix_index[e[0]]
        w_arr = np.array(adj_matrix_weight[row_indx])
        arr = np.array(adj_matrix[row_indx])
        w_arr[col_indx] = mfp_edge_cost[e[0],e[1]]
        arr[col_indx] = 1
        adj_matrix[row_indx] = arr
        adj_matrix_weight[row_indx] = w_arr
    print(dummy_node_adj_matrix_weight)
    print("Adjacency Matrix: \n", adj_matrix_weight)
    # print(adj_matrix)
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
        idx = source_traces[source].index(source_node)
        if idx == 0:
            dist[source] = np.sum(e_matrix[source])
            appended_graph[("PH",source)] = dist[source]
        else:
            node_lst = source_traces[source]
            edges = [(node_lst[i], node_lst[i+1]) for i in range(idx,len(node_lst)-1)]
            e_matrix_index = [edge_map[i] for i in edges]
            energy = e_matrix[source]
            dist[source] = np.sum(energy[e_matrix_index])
            appended_graph[("PH",source)] = np.sum(energy[e_matrix_index])
        source_lst.append(source)
    for edge in mfp_edge_cost:
        appended_graph[edge]=mfp_edge_cost[edge]
    final_trace = None
    while len(queue_lst):
        edge=find_min_edge_cost(source_lst, mfp_edge_cost, queue_lst)
        if edge is None:
            break
        # get indices where energy is not 0 and then find if the destination is within the trace
        else:
            print(edge)
            if edge[1] in destination_traces:              
                p = prev[edge[0]]
                alt = mfp_edge_cost[edge]
                if alt < dist[edge[1]]:
                    dist[edge[1]] = dist[edge[0]] + alt
                    prev[edge[1]] = edge[0]
                    final_trace = edge[1]
                    queue_lst.remove(edge[1])
                    break
            else:
                alt = dist[edge[0]]
                dist[edge[1]] = mfp_edge_cost[edge]
                prev[edge[1]] = edge[0]
                source_lst = [edge[0]]
                queue_lst.remove(edge[1])
                combo = [(edge[1],check) for check in queue_lst if (edge[1], check) in appended_graph]
                #get every MFP-edge with path_j-> path_k where path_j is edge[1]
                for check_edge in combo:
                    #if dist of path_j is >= max then alt is cost[path_j->path_k]
                    if dist[check_edge[0]] < sys.maxsize:
                        alt += dist[check_edge[0]] + mfp_edge_cost[check_edge]
                    else:
                        alt +=  mfp_edge_cost[check_edge]
                    if alt < dist[check_edge[1]]:
                        # cost is updated if alt is less than the previous cost edge of dist[path_k]
                        dist[check_edge[1]] = alt
                        prev[check_edge[1]] = check_edge[0] 
                        final_trace = check_edge[1]
                        if final_trace in destination_traces:
                            break
                        else:
                            p = prev[check_edge[0]]
                            alt = dist[p]
    if final_trace is None:
        return None, None
    shortest_path_cost = dist[final_trace]
    shortest_path =[final_trace]    
    while final_trace is not None:
        final_trace = prev[final_trace]
        if final_trace is not None:
            shortest_path.append(final_trace)
    print(shortest_path[::-1])
    print(shortest_path_cost)
    return shortest_path[::-1], shortest_path_cost

    
