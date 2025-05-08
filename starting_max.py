import numpy as np
import pandas as pd

def build_column_name(n):
    column_names = []
    for i in range(1, n+1):
        column_names.extend([f"node_id{i}", f"id{i}", f"title{i}", f"finish{i}", f"tag_id{i}", f"tag{i}"])
    column_names.append("max_chain")
    return column_names


def update_relations_df_max(df, matrix, starting):
    for n in range(0, starting):
        max = matrix[:, n].max()
        if max > 2:
            df.loc[df["node_id1"] == n, "max_chain"]= max

    return df




def update_max(df, matrix, starting = 0):
    #setting the maxium possible maxium of each node
    #NOTE The logic here is to loop through all nodes grouped by  finish date and check the max chain of each group of nodes
    #they are connected to that have a finish date before the finish date of the starting node.
    #The max chain of that node is that maximum + 1 + the number of possible nodes that have the same finish date
    df["max_chain"] = 2
    if starting > 0:
        df = update_relations_df_max(df, matrix, starting)
    #TODO Think of better will to do this. I don't like looping df
    list_nodes = sorted(df[df["node_id1"] > starting]["node_id1"].unique())
    for node in list_nodes:
        #group of nodes relations that have the same finish date
        helper = df[df["node_id1"] == node].copy()
        max_node = 2
        n = 2
        # number of unique nodes
        while not helper.empty:
            n += 1
            helper = helper.merge(df, left_on=f"node_id{n - 1}", right_on="node_id1")
            helper.drop(
                columns=["node_id1_y", "id1_y", "title1_y", "finish1_y", "tag_id1_y", "tag1_y", "max_chain_x"],
                inplace=True)
            new_column = build_column_name(n)
            helper.columns = new_column

            condition = condition = pd.Series(True, index=helper.index)
            for i in range (1, n):
                condition = condition & ((helper[f"id{i}"] != helper[f"id{n}"]) & 
                                        (helper[f"tag_id{i}"] != helper[f"tag_id{n}"]))
                   
            helper = helper[condition]
            next_helper = helper[helper[f"finish{n - 1}"] == helper[f"finish1"]]
            helper = helper[helper[f"finish{n}"] != helper[f"finish1"]]
            loop_max = helper["max_chain"].max() + n - 2
            if  not pd.isna(loop_max):
                if loop_max > max_node:
                    max_node = loop_max

            helper = next_helper

        if max_node > 2:
            df.loc[df["node_id1"] == node, 'max_chain'] = max_node



    #update the relations matrix the max_chain of the nodes
    max_chain_values = df.groupby("node_id1")["max_chain"].max()
    for  node_id, max_value in max_chain_values.items():
        print("node_id: "+ str(node_id) + "max " + str(max_value))
        matrix[matrix[:, node_id] > 0, node_id] = max_value

    return matrix