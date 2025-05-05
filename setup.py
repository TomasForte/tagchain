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
    df = df[["id1", "title1", "finish1", "tag_id1", "tag1",
             "id2", "title2", "finish2", "tag_id2", "tag2",
             "node_id1", "node_id2"]] 
    df = df[df['tag1'] != df['tag2']] # tag2 can be the same as tag1
    df = df.sort_values(by=["finish2", "finish1", "id1"], ascending=False)

    return df


def setup_shared_variables(df, df_nodes):
    """Initialize relations matrix"""
   
    #NOTE the index of the array and matrix are the node_id
    # Create index array with the id and tag_id of each node
    nodes = df_nodes[["id", "tag_id"]].to_numpy(dtype = "int64")
    nodes_size = nodes.shape[0] 

    # Create relations matrix
    matrix = np.zeros((nodes_size, nodes_size), dtype=int)
    matrix[df["node_id1"].values, df["node_id2"].values] = 1


    #setting the maxium possible maxium of each node
    #NOTE The logic here is to loop through all nodes grouped by  finish date and check the max chain of each group of nodes
    #they are connected to that have a finish date before the finish date of the starting node.
    #The max chain of that node is that maximum + 1 + the number of possible nodes that have the same finish date
    df["max_chain"] = 2
    #TODO Think of better will to do this. I don't like looping df
    for date in sorted(df["finish1"].unique(), reverse=True):
        #group of nodes relations that have the same finish date
        helper = df[df["finish1"] == date].copy()
        # number of unique nodes
        entries_same_date = helper[helper["finish1"] == helper["finish2"]]["id2"].nunique()
        # remove relations that link nodes with the same finish date
        helper = helper[helper["finish1"] != helper["finish2"]]
        uniquenode = helper["node_id2"].unique()        
        # merge section the relations df        
        helper = df[df["node_id1"].isin(uniquenode)]


        # get max fot section
        max_chain = helper["max_chain"].max()
        if pd.isna(max_chain) or max_chain < 2:
            print("foda-se")
        # if no max the max is the default else the max_chain + 1
        max_chain = 2 if pd.isna(max_chain) else max_chain + 1
        # add the number of nodes with the same date as in the best case scenario they can link with ona another
        # and then follow the path of the max_chain
        max_chain_start =  max_chain + entries_same_date

        if max_chain_start >= 2:
            df.loc[df["finish1"] == date, 'max_chain'] = max_chain_start



    #update the relations matrix the max_chain of the nodes
    max_chain_values = df.groupby("node_id1")["max_chain"].max()
    for  node_id, max_value in max_chain_values.items():
        print("node_id: " + str(max_value))
        matrix[matrix[:, node_id] > 0, node_id] = max_value

    test = df[df["id1"]==48512]
    test2 = df[df["tag_id2"]==140]

    #build matrix with the nodes out based on each node
    matrix_out = np.zeros((nodes_size, nodes_size), dtype=bool)
    for i  in range(0, nodes_size):
        nodes_out = (df_nodes["id"] == df_nodes.iloc[i]["id"]) | (df_nodes["tag_id"] == df_nodes.iloc[i]["tag_id"])
        matrix_out[i,:]= nodes_out
    

    return nodes, nodes_size, matrix, matrix_out

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


