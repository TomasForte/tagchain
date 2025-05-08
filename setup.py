import numpy as np
import time
import multiprocessing
import multiprocessing_max_update
import multiprocessing_main_loop
import logging
import cProfile
import pstats
import tracemalloc
#import psutil
import pandas as pd
import starting_max

def build_column_name(n):
    column_names = []
    for i in range(1, n+1):
        column_names.extend([f"node_id{i}", f"id{i}", f"title{i}", f"finish{i}", f"tag_id{i}", f"tag{i}"])
    column_names.append("max_chain")
    return column_names

def prepare_data(df_tv, df_tags): 
    """Merge and clean data, add node IDs"""

    # join tv and tags dataframe
    df = df_tv.merge(df_tags, left_on="title", right_on="title", how="right")
    df = df[["id_x", "title", "finish", "tag_id", "tag"]]
    df.columns = ["id", "title", "finish", "tag_id", "tag"]
    df = df.sort_values(by=["finish", "id"], ascending=False)
    

    # Add node_id
    #TODO there is a possibility that some node here won't be in the df_relations they will still appear in the 
    #relations matrix. It might be a good idea to only consider the nodes that appear in the final df 
    df_nodes = df.copy()
    df_nodes.reset_index(inplace=True, drop=True)
    df_nodes.reset_index(inplace=True, names="node_id")

    return df, df_nodes


def build_relations_dataframe(df, df_nodes):
    """Build relations between nodes based on tags and IDs."""
    # Merge by tag_id
    df = df_nodes.merge(df, left_on="tag_id", right_on="tag_id")
    df = df.drop(columns=["tag_y"])
    df.columns = ["node_id1", "id1", "title1", "finish1", "tag_id1", "tag1", "id2", "title2", "finish2"]
    df = df[df['id2'] != df['id1']] # id2 can't be the same as id1
    df = df[df['finish2'] >= df['finish1']] # finish2 must be after finish1

    # Merge by id
    df = df.merge(df_nodes, left_on="id2", right_on="id")
    df = df.drop(columns=["id", "title", "finish"])
    df.columns = ["node_id1", "id1", "title1", "finish1", "tag_id1", "tag1",
                  "id2", "title2", "finish2", "node_id2", "tag_id2", "tag2"]
    df = df[["node_id1", "id1", "title1", "finish1", "tag_id1", "tag1",
             "node_id2", "id2", "title2", "finish2", "tag_id2", "tag2"]] 
    df = df[df['tag1'] != df['tag2']] # tag2 can be the same as tag1
    df = df.sort_values(by=["finish2", "finish1", "id1"], ascending=False)

    return df


def get_nodes(df_nodes):
    """Initialize relations matrix"""
   
    #NOTE the index of the array and matrix are the node_id
    # Create index array with the id and tag_id of each node
    nodes = df_nodes[["id", "tag_id"]].to_numpy(dtype = "int64")
    nodes_size = nodes.shape[0] 

    #build matrix with the nodes out based on each node
    matrix_out = np.zeros((nodes_size, nodes_size), dtype=bool)
    for i  in range(0, nodes_size):
        nodes_out = (df_nodes["id"] == df_nodes.iloc[i]["id"]) | (df_nodes["tag_id"] == df_nodes.iloc[i]["tag_id"])
        matrix_out[i,:]= nodes_out

    return nodes, nodes_size, matrix_out

def get_matrix(df, nodes_size):

    # Create relations matrix
    matrix = np.zeros((nodes_size, nodes_size), dtype=int)
    matrix[df["node_id1"].values, df["node_id2"].values] = 1

    matrix = starting_max.update_max(df, matrix)



    return matrix

#TODO instead of using df_notes i could use df_relations to get with nodes that
#are connected to another node. I could also start the chains where alreay two elements
def build_chain_list(df_nodes, matrix_out):
    """building the starting chain by node."""

    #prepare the chain list
    grouped_df = df_nodes.groupby("node_id")
    chains_by_node = []

    for node_id, group in grouped_df:
        chains = []
        
        for _, row in group.iterrows():
            current_node_id = row["node_id"]
            current_id = row["id"]
            current_tag_id = row["tag_id"]

            chains.append((
                current_node_id,         # node_id of last element in the chain
                (current_id,),            # chain of id
                (current_tag_id,),        # chain of tag_id
                matrix_out[current_node_id,:],    # list of unique node_ids
                1                        # elements in the chain
            ))
        
        chains_by_node.append([node_id, chains])


    return chains_by_node


