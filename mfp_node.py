import numpy as np
import collections

def get_edge_count(traces, trace_edges,edge_count):
        '''
        Purpose: Computes total edge count from all existing traces
        Returns: trace_edges (dict) trace mapped to its edges
                    edge_count (dict) edge mapped to the number of time it appears in a trace
        '''
        # loops through all the traces and generates edges in each trace
        # the second loop iterates through each edge of the current trace and increments the count of an edge
        for node in traces:
            path= traces[str(node)]
            trace_edges[node]= [(path[i], path[i+1]) for i in range(len(path)-1)]
            for edge in trace_edges[node]:
                if edge not in edge_count:
                    edge_count[edge] = 1
                else:
                    edge_count[edge]+=1
        return trace_edges, edge_count
    
def get_candidate_mfp(traces, edge_count, edges, beta):
    '''
    Purpose: Identifies candidate MFP nodes based on the trace
    Return: candidate_mfp (dict) trace mapped to number of edges
    '''
    # first loop iterates through all the traces
    # second loop iterates through all edges in the current trace
    # if an edge that exists in a trace has an overall edge_count of less than beta than the trace is removed
    # the final candidate_mfp hashmap has the traces mapped to the number of edges
    candidate_mfp ={}
    for node in traces:
        candidate_mfp[node]= len(traces[node])-1
        for edge in edge_count:
            if edge in edges[node] and edge_count[edge] < beta:
                del candidate_mfp[node]
                break
    
    return candidate_mfp
    
def set_energy_matrix(traces, candidate_mfp, matrix_energy, edge_graph, matrix, graph_type):
    '''
    Purpose: defines a matrix with energy cost set to corresponding edges
    Return: matrix_energy (list of list) 
    '''
    if graph_type ==1:
        energy = {'P1': [2,5,3],
                'P2': [1,3],
                'P3': [2,3,4],
                'P4': [5,6,6],
                'P5': [5,7,7]}
    else:
        energy= {'P1':[2,9],
                'P2': [1,1],
                'P3': [3,2,4,3],
                'P4': [7,9,2],
                'P5': [9,2,2,7],
                'P6': [3,2,2,2],
                'P7': [1,1,1],
                'P8': [1,1],
                'P9': [1,1],
                # 'P10': [1,-3,-2,1,1,1]
                }
    
    
    #iterates through all the traces(candidate mfp) and extracts indices where idx_matrix (row) !=0
    # the indices are then used to set the energy cost in matrix_energy (row)
    
    
    for i, j in enumerate(candidate_mfp):
        trace= traces[j]
        edges = [(trace[idx1],trace[idx1+1]) for idx1 in range(len(trace)-1)]
        row_indices = np.where(matrix[i] != 0)[0]
        cols=[]
        for e in edges:
            cols.append(edge_graph[e])
        cols = np.array(cols)
        curr_array = np.array(matrix_energy[i])
        curr_array[cols] = np.array(energy[j])
        # curr_array = np.array(matrix_energy[i])
        
        # curr_array[row_indices] = np.array(energy[j])
        matrix_energy[i] = curr_array
    return matrix_energy




        

def find_mfp_node(traces,graph_type,beta,source_node, destination_node,og_graph_edges):
    '''
    Purpose: Identifies the MFP nodes based on trace 
    Return: list of mfp nodes [P1,P2,P3] 
    '''
    edge_count= {} 
    edges=collections.defaultdict(list) 
    edges,edge_count = get_edge_count(traces,edges,edge_count)  
    # edges is a hashmap with traces mapped to their edges
    # edge_count is hashmap with unique edges mapped to the number of times the edge appears in trace
    
    candidate_mfp= get_candidate_mfp(traces, edge_count, edges,beta)
    # gets the candidate mfp with by removing traces that are not FP
    # candidate_mfp has traces mapped to number of edges it has 
    # for example if an edge appears in a trace and the edge count < beta then the trace is not a FP    
    
    destination_traces = []
    source_traces = []
    for i in traces:

        if destination_node in traces[i]:
            destination_traces.append(i)
        if source_node in traces[i] and i not in source_traces:
                source_traces.append(i)
    indices={}    
    
    
    for path in candidate_mfp:
        indices[path] = list(range(1, len(edges[path])+1))
    # indices has traces mapped to a list of integers that represents the sequence of edges

    graph_edges = { edges:i for i, edges in enumerate(og_graph_edges)}    

    bi_directional = {(edges[-1], edges[0]):i + len(graph_edges) for i, edges in enumerate(graph_edges)}
    for i,k in bi_directional.items():
        graph_edges[i] = k    
    
    
    #bidirectional edges of a graph mapped to a unique value -> this unique value will serve as a column index in the matrices
  
    matrix = np.zeros((len(candidate_mfp), len(graph_edges)))
    energy_matrix = np.zeros((len(candidate_mfp), len(graph_edges)))
    #initialized 2 matrices
    #two matrices created, one for storing the sequuence of the edges and one for energy consumption of each edge
    
    
    for i, path in enumerate(candidate_mfp):
        update_idx = np.array([graph_edges[p] for p in edges[path]]) # extracts the index that needs to be updated in the matrix row(i)
        row = np.array(matrix[i])
        row[update_idx] = np.array(indices[path])
        matrix[i] = row
    
    
    # this extracts the indices of the columns in a matrix row, where the columns represents the edges and rows represents the traces.
    # the indices in a row are set to non zero value if an edge exists. The values are set based on the order and is non-decreasing.
    
    candidate_traces = [trace for trace in candidate_mfp]
    set_energy_matrix(traces, candidate_mfp, energy_matrix, graph_edges, matrix, graph_type)
    # the energy matrix has non zero values in each row where an edge exists
    
    remove_rows =[]
    for i, path_i in enumerate(candidate_traces):
        if i >= len(candidate_mfp)-1:                
            break
        for j in range(i+1,len(candidate_traces)):
            idx_i = np.where(matrix[i] != 0)[0].tolist()
            idx_j = np.where(matrix[j] != 0)[0].tolist()
            if set(idx_i).issubset(set(idx_j)):
                remove_rows.append(i)
                del candidate_mfp[path_i]
            elif set(idx_j).issubset(set(idx_i)):
                remove_rows.append(j)
                del candidate_mfp[candidate_traces[j]]
                if candidate_traces[j] in destination_traces:
                    destination_traces.remove(candidate_traces[j])
    # the outer loop iterates through the matrix from the first to the last second row.
    # inner loop iterates through the matrix from the next row of the outer loop to the last row.
    # rows/candidate mfp nodes are deleted if a row is a proper subset of another row
    
    for i in remove_rows:
        energy_matrix = np.delete(energy_matrix, i,axis=0)
        matrix=np.delete(matrix, i,axis=0)
    
    return candidate_mfp, energy_matrix, matrix, graph_edges, destination_traces, source_traces

