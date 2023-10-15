import numpy as np
import math
import random
import sys
import collections
import argparse

class graph:
    def __init__(self,gdict=None):
        if gdict is None:
            gdict = {}
        self.gdict = gdict
        
    def get_edge_count(self,traces, trace_edges,edge_count):
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
    
    
    def get_candidate_mfp(self, traces, edge_count, edges, beta):
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
    
    def set_energy_matrix(self, candidate_mfp, matrix_energy, matrix):
        '''
        Purpose: defines a matrix with energy cost set to corresponding edges
        Return: matrix_energy (list of list) 
        '''
        if args.graph ==1:
            energy = {'P1': [2,5,3],
                    'P2': [1,3],
                    'P3': [2,3,4],
                    'P4': [5,6,6],
                    'P5': [5,7,7]}
        else:
            energy= {'P1':[2,2],
                    'P2': [1,1],
                    'P3': [3,2,4,3],
                    'P4': [7,9,2],
                    'P5': [9,2,2,7],
                    'P6': [3,2,2,2],
                    'P7': [1,1,1],
                    'P8': [1,1],
                    'P9': [1,1]
                    }
        
        #iterates through all the traces(candidate mfp) and extracts indices where idx_matrix (row) !=0
        # the ideces are then used to set the energy cost in matrix_energy (row)
        for i, j in enumerate(candidate_mfp):
            row_indices = np.where(matrix[i] != 0)[0]
            curr_array = np.array(matrix_energy[i])
            curr_array[row_indices] = np.array(energy[j])
            matrix_energy[i] = curr_array
        return matrix_energy
        
    
    def find_mfp_node(self, traces,beta=1):
        '''
        Purpose: Identifies the MFP nodes based on trace 
        Return: list of mfp nodes [P1,P2,P3] 
        '''
        edge_count= {} 
        edges=collections.defaultdict(list) 
        edges,edge_count = self.get_edge_count(traces,edges,edge_count)     
        # edges is a hashmap with traces mapped to their edges
        # edge_count is hashmap with unique edges mapped to the number of times the edge appears in trace
        
        
        candidate_mfp= self.get_candidate_mfp(traces, edge_count, edges,beta)
        # gets the candidate mfp with by removing traces that are not FP
        # candidate_mfp has traces mapped to number of edges it has 
        # for example if an edge appears in a trace and the edge count < beta then the trace is not a FP
        
        indices={}
        for path in candidate_mfp:
            indices[path] = list(range(1, len(edges[path])+1))
        # indices has traces mapped to a list of integers that represents the sequence of edges

        graph_edges = { edges :i for i, edges in enumerate(self.edges())}
        bi_directional = {(edges[-1], edges[0]):i+ len(graph_edges) for i, edges in enumerate(graph_edges)}
        for i,k in bi_directional.items():
            graph_edges[i] =k
        #bidirectional edges of a graph mapped to a unique value -> this unique value will serve as a column index in the matrices
        
        matrix = np.zeros((len(candidate_mfp), len(graph_edges)))
        energy_matrix = np.zeros((len(candidate_mfp), len(graph_edges)))
        #initialized 2 matrices
        #two matrices created, one for storing the sequuence of the edges and one for energy consumption of each edge
        
        for i, path in enumerate(candidate_mfp):
            update_idx = np.array([graph_edges[i] for i in edges[path]]) # extracts the index that needs to be updated in the matrix row(i)
            row = np.array(matrix[i])
            row[update_idx] = np.array(indices[path])
            matrix[i] = row
        # this extracts the indices of the columns in a matrix row, where the columns represents the edges and rows represents the traces.
        # the indices in a row are set to non zero value if an edge exists. The values are set based on the order and is non-decreasing.
        
        candidate_traces = [trace for trace in candidate_mfp]
        self.set_energy_matrix(candidate_mfp, energy_matrix, matrix)
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
        # the outer loop iterates through the matrix from the first to the last second row.
        # inner loop iterates through the matrix from the next row of the outer loop to the last row.
        # rows/candidate mfp nodes are deleted if a row is a proper subset of another row
        for i in remove_rows:
            energy_matrix = np.delete(energy_matrix, (i),axis=0)
            matrix=np.delete(matrix, i,axis=0)
        return candidate_mfp, energy_matrix, matrix
    

    def find_mfp_edges(self, mfp_nodes,idx_matrix):
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


    def exp_mfp_edges(self, mfp_nodes, energy_matrix,idx_matrix, mfp_edges):
        '''
        Purpose: Determine MFP-edges based on proposed definition
        Return: update matrix, traces, mfp_edges, and mfp_nodes corresponding to proposed definition of MFP-edges
        '''
        # get union of two matrix rows and check if the another row is a subset of the union 
        nodes = {node:i for i, node in enumerate(mfp_nodes)}
        edge_dict = collections.defaultdict(list)
        for edges in mfp_edges:
            edge_dict[edges[0]].append(edges[-1])
            
        deleted_nodes =[]
        for path_i in edge_dict.keys():
            edge_i = edge_dict[path_i]
            for edge_j in edge_i:
                # if a trace is a key in the hashmap 
                if edge_j in edge_dict:
                    check = edge_dict[edge_j]
                    intersection = set(edge_i).intersection(check)
                    if len(intersection) > 0:
                        del mfp_nodes[edge_j]
                        deleted_nodes.append(edge_j)     
        
        num_edges = len(mfp_edges) -1
        for i in deleted_nodes:
            energy_matrix = np.delete(energy_matrix, nodes[i], axis=0)
            idx_matrix = np.delete(idx_matrix, nodes[i], axis=0)
            del edge_dict[i]
            while num_edges>= 0:
                if i in mfp_edges[num_edges]:
                    mfp_edges.remove(mfp_edges[num_edges])
                num_edges-=1
        return e_matrix, idx_matrix, mfp_nodes, mfp_edges
    
    def edges(self):
        return self.findedges()
    def vertices(self):
        return self.findvertices()
    def findvertices(self):
        vertices = [vertx for vertx in self.gdict]
        return vertices
    def findedges(self):
        edgename = []
        for vrtx in self.gdict:
            for nxtvrtx in self.gdict[vrtx]:
                if (nxtvrtx, vrtx) not in edgename:
                    edgename.append((vrtx, nxtvrtx))
        return edgename
             
'''
1) identify traces that can be a mfpg node
2) identify mfpg edge between nodes
3) make sure it is a directed edge
'''
if __name__ == "__main__":
    parser =  argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--graph', type = int, default=1, help='1 or 2')
    args=parser.parse_args()
    if args.graph == 1:
        graph_elements = {     
                    "A" : ["B","C","E"],
                    "B" : ["A", "E"],
                    "C" : ["A", "D"],
                    "D" : ["C","E"],
                    "E" : ["A","B","D","F"],
                    "F" : ["E","G"],
                    "G" : ["F"]
                    
                }
        traces = {
            "P1" : ["A", "B", "E", "F"],
            "P2" : ["B","E","F"],
            "P3" : ["A","C","D","E"],
            "P4" : ["C","D","E","F"],
            "P5" : ["D","E","F","G"]
        }
    elif args.graph ==2:
        graph_elements={
                    "n1": ["n2"],
                    "n2": ["n1", "n3", "n6"],
                    "n3":["n2", "n4", "n7"],
                    "n4":["n5","n3", "n8"],
                    "n5":["n4"],
                    "n6":["n2", "n9"],
                    "n7":["n3", "n10"],
                    "n8":["n4","n11"],
                    "n9":["n6", "n10"],
                    "n10":["n7", "n9","n11"],
                    "n11":["n8", "n10"],
                    "n12": ["n13"],
                    "n13": ["n12", "n9"],
                    "n14": ["n11"]
        }
        
        traces ={
            "P1": ["n1", "n2", "n3"],
            "P2": ["n1", "n2", "n6"],
            "P3": ["n2", "n6", "n9", "n10","n11"],
            "P4": ["n2", "n3", "n4", "n5"],
            "P5": ["n2", "n3", "n7", "n10", "n11"],
            "P6": ["n10", "n11", "n8", "n4", "n5"],
            "P7": ["n2", "n6", "n9", "n13"],
            "P8": ["n9", "n13", "n12"],
            "P9": ["n10", "n11", "n14"]
        }
    else:
        print("PLEASE ENTER VALID INPUT")
        sys.exit()
        
    g = graph(graph_elements)
    vertices = g.findvertices()
    mfp_vertices, e_matrix, i_matrix= g.find_mfp_node(traces)
    mfp_edges = g.find_mfp_edges(mfp_vertices, i_matrix) #edge_list, matrix, mfp_nodes
    e_matrix, idx_matrix, mfp_nodes, mfp_edges = g.exp_mfp_edges(mfp_vertices, e_matrix, i_matrix, mfp_edges)
    print("CURRENT MFP EDGES:", mfp_edges)
    print("CURRENT MFP NODES: ", list(mfp_nodes.keys()))



