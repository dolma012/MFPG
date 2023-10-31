import numpy as np
import sys




def find_mfp_edges(mfp_nodes,idx_matrix):
    nodes = [node for node in mfp_nodes]
    edges=[]
    for i in range(len(mfp_nodes)):
        for j in range(i+1,len(mfp_nodes)):
            idx1= set(np.where(idx_matrix[i] !=0)[0].tolist())
            idx2 = set(np.where(idx_matrix[j] !=0)[0].tolist())
            intersection_col = list(idx1.intersection(idx2))
            if len(intersection_col) >0:                    
                vals_1 = idx_matrix[i, np.array(list(idx1))].tolist()
                vals_2 = idx_matrix[j, np.array(list(idx2))].tolist()
                if idx_matrix[i, max(intersection_col)] == max(vals_1):
                    edges.append((nodes[i], nodes[j]))
                else:
                    edges.append((nodes[j], nodes[i]))
                if idx_matrix[i,int(max(intersection_col))] == max(vals_1) and idx_matrix[j,max(intersection_col)] == max(vals_2):
                    edges.pop()
                if idx_matrix[j,min(intersection_col)] == min(vals_2) and  idx_matrix[i,min(intersection_col)] == min(vals_1):
                    edges.pop()
    
    # outer loop iterates through the mfp_nodes and the outer loop iterates from the next mfp_node of the outer loop
    # get the indices of both rows i and j where values are non-zero.
    # list of intersecting columns are stored
    # if no columns intersect then the two paths dont have an mfp edge
    # else: get the index values of both rows from the idx_matrix. 
    # Where the values are based on the order they appear in the path
    # if the last intersecting value of row i is equal to the max of all the indices than  node i -> node j
    # else: mfp j -> mfp i
    #the order of how the mfp_edges are appended to the edges list matter. If the intersection edge is the last edge of trace i than  i->j
    return edges



def mfp_edge_cost(e_matrix, mfp_nodes, mfp_edges, edge_graph):
    '''
    Purpose: Compute MFP-edge energy consumption based on proposed method
    Return: edge_cost (dict) with keys as edges ('P1','P2') and values as total energy consumption
    '''
    mfp_edge_cost = {edge: sys.maxsize for edge in mfp_edges}
    mfp_nodes_lst = [node for node in mfp_nodes]
    final_matrix = {}
    mfp_nodes=set()
    for edge in mfp_edges:
        
        idx1 = mfp_nodes_lst.index(edge[0])
        idx2 = mfp_nodes_lst.index(edge[1])
        final_matrix[edge[0]] = e_matrix[idx1]
        final_matrix[edge[1]] = e_matrix[idx2]
        mfp_nodes.add(edge[0])
        mfp_nodes.add(edge[1])
        idx_1 = set(np.where(e_matrix[idx1] !=0)[0])           
        idx_2 = set(np.where(e_matrix[idx2] !=0)[0])
        intersect_set = idx_1.intersection(idx_2)
        
        # still need to work on what needs to be subtracted from total sum
        # back tracking to remove unecessary edge cost
        if len(intersect_set) >0:
            # sum_shared = sum(list(e_matrix[idx1,list(intersect_set)] + e_matrix[idx2,list(intersect_set)])) / (2*(len(intersect_set)))
            sum_shared =  sum((e_matrix[idx2,[i]] + e_matrix[idx1, [i]])/2 for i in intersect_set)
            mfp_edge_cost[edge] = np.sum(e_matrix[idx2]) - sum_shared

    return mfp_edge_cost, list(mfp_nodes),final_matrix
    
    
    