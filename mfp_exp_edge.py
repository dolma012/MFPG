import numpy as np
import math
import random
import sys
import collections
import argparse



def exp_mfp_edges(mfp_nodes, energy_matrix,idx_matrix, mfp_edges, source_traces, destination_traces, traces):
    '''
    Purpose: Determine MFP-edges based on proposed definition
    Return: update matrix, traces, mfp_edges, and mfp_nodes corresponding to proposed definition of MFP-edges
    '''
    # get union of two matrix rows and check if the another row is a subset of the union 
    
    nodes = {node:i for i, node in enumerate(mfp_nodes)}
    edge_dict = collections.defaultdict(list)
    for edges in mfp_edges:
        edge_dict[edges[0]].append(edges[-1])
    #edge_dict has MFP-node as keys mapped to all nodes it forms a UFP with as values
    deleted_nodes =[]
    keys = [x for x in edge_dict.keys()]
    for path_i in keys:
        edge_i = edge_dict[path_i]
        for edge_j in edge_i:
            # if a trace is a key in the hashmap 
            if edge_j in keys:
                check = set(edge_dict[edge_j])
                intersection = set(edge_i).intersection(check)
                if len(intersection) > 0:
                    del mfp_nodes[edge_j]
                    deleted_nodes.append(edge_j)
                    del edge_dict[edge_j] 
    
    num_edges = len(mfp_edges) - 1
    for i in deleted_nodes:
        if i in source_traces:
            source_traces.remove(i)
        if i in destination_traces:
            destination_traces.remove(i)
        energy_matrix = np.delete(energy_matrix,nodes[i] , axis=0)
        idx_matrix = np.delete(idx_matrix, nodes[i], axis=0)
        del edge_dict[i]
        while num_edges>= 0:
            if i in mfp_edges[num_edges]:
                mfp_edges.remove(mfp_edges[num_edges])
            num_edges-=1
    source_traces_dict =  {}
    for i in source_traces:
        source_traces_dict[i] = traces[i]
    return energy_matrix, idx_matrix, mfp_nodes, mfp_edges, source_traces_dict, destination_traces
