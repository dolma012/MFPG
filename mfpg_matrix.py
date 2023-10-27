import numpy as np
import math
import random
import sys
import collections
import argparse
import mfp_node
import mfp_edge
import mfp_exp_edge
import shortest_dist



class graph:
    def __init__(self,gdict=None):
        if gdict is None:
            gdict = {}
        self.gdict = gdict
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
    parser.add_argument('--beta',type= int, default = 1, help="threshold")
    parser.add_argument('--destination', type=str)
    parser.add_argument('--source', type=str)
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
            "P5": ["n2", "n3", "n7", "n10", "n11"],
            "P6": ["n10", "n11", "n8", "n4", "n5"],
            "P7": ["n2", "n6", "n9", "n13"],
            "P8": ["n9", "n13", "n12"],
            "P9": ["n10", "n11", "n14"],
            "P1": ["n1", "n2", "n3"],
            "P2": ["n1", "n2", "n6"],
            "P3": ["n2", "n6", "n9", "n10","n11"],
            "P4": ["n2", "n3", "n4", "n5"],
            # "P10": ["n5", "n4", "n3", "n7", "n10","n11", "n14"]
        }
    else:
        print("PLEASE ENTER VALID INPUT")
        sys.exit()
        
    g = graph(graph_elements)
    vertices = g.findvertices()
    source_node = "n10"
    destination_node = "n5"
    mfp_vertices, e_matrix, i_matrix, edge_map, destination_traces, source_traces = mfp_node.find_mfp_node(traces, args.graph, args.beta,source_node,destination_node, g.findedges())
    mfp_edges = mfp_edge.find_mfp_edges(mfp_vertices, i_matrix) #edge_list, matrix, mfp_nodes
    e_matrix, idx_matrix, mfp_nodes, mfp_edges, source_traces, destination_traces = mfp_exp_edge.exp_mfp_edges(mfp_vertices, e_matrix, i_matrix, mfp_edges,source_traces, destination_traces)
   
    mfp_edge_cost, mfp_nodes,e_matrix = mfp_edge.mfp_edge_cost(e_matrix, mfp_nodes, mfp_edges, edge_map)
    mfp_shortest_path, mfp_shortest_p_cost = shortest_dist.compute_shortest_path(mfp_edge_cost,mfp_nodes,e_matrix, edge_map,destination_traces,source_traces )
    
    # print(mfp_shortest_path, mfp_shortest_p_cost)
    # print("CURRENT MFP EDGE COST:", mfp_edge_cost)
    # print("CURRENT MFP EDGES:", mfp_edges)
    # print("CURRENT MFP NODES: ", mfp_nodes)
    
    
    




