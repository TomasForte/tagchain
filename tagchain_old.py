import numpy as np
import time
import multiprocessing
import multiprocessing_max_update
import multiprocessing_main_loop
import logging
import cProfile
import pstats
import tracemalloc
import psutil
import pandas as pd

def get_tag_chain(df_tv, df_tags, wanted_chain, number_processes, max_task_per_process ):
    #merge a-p tags to mysql entries to get finish dates
    df = df_tv.merge(df_tags, left_on = "title", right_on = "title", how = "right")
    df = df[["id_x", "title","finish","tag_id","tag"]]
    df.columns = ["id", "title","finish","tag_id","tag"]
    df  = df.sort_values(by = ["finish","id"], ascending = False)
    df_list = df.copy()

    #add node_id to list dataframe add node_id to both side of the relations df
    #TODO there is a possibility that some node here won't be in the final df however they will still appear in the 
    #relations matrix. It might be a good idea to only consider the nodes that appear in the final df 
    df_list.reset_index(inplace = True, drop = True)
    df_list.reset_index(inplace = True, names = "node_id")


    # merge by tag_id (get possible next movie that has tag_id)
    df = df_list.merge(df, left_on = "tag_id", right_on = "tag_id")
    df = df.drop(columns = ["tag_y"])
    df.columns = ["node_id1", "id1", "title1", "finish1", "tag_id1", "tag1", "id2", "title2", "finish2"]
    # id2 can't be the same as id and it finish date must not be before id1
    df = df[df['id2'] != df['id1']]
    df = df[df['finish2'] >= df['finish1']]


    # merger by id( get possible next tag)
    df = df.merge(df_list, left_on = "id2", right_on = "id")
    df = df.drop(columns = ["id", "title", "finish"])
    df.columns = ["node_id1", "id1", "title1", "finish1", "tag_id1", "tag1",
                  "id2", "title2", "finish2", "node_id2", "tag_id2", "tag2"]
    df = df[["id1", "title1", "finish1", "tag_id1", "tag1", 
             "id2", "title2", "finish2", "tag_id2", "tag2",
             "node_id1", "node_id2"]]
    #tag2 can be the same as tag1
    df = df[df['tag1'] != df['tag2']]
    df = df.sort_values(by = ["finish2", "finish1", "id1"], ascending = False)


    #list of possible starting id
    list_shows = df[['id1', "finish1"]].sort_values("finish1", ascending = False)["id1"].unique()



    df["max_chain"] = 2
    matrix = np.zeros((len(df_list.index), len(df_list.index)), dtype = int)
    matrix[df["node_id1"].values, df["node_id2"].values] = df["max_chain"].values


    #setting the maxium possible maxium of each node
    #TODO IF two or more nodes that finish in the same day can be connect the maxium may not be correct
    #because the first to be looped through will connect to the other ones while their maxium is still 2
    df["max_chain"] = 2
    #TODO Think of better will to do this. I don't like looping df
    max_chain = 2
    #starting from the most recent 
    for index in range(0,len(list_shows)):
        helper = df[df["id1"]==list_shows[index]].copy()
        helper = helper.merge(df, left_on=[f"tag_id2",f"id2"], right_on=["tag_id1","id1"])
        helper = helper[helper['node_id1_x'] != helper['node_id2_y']]
        max_chain_start = helper["max_chain_y"].max() + 1

        if max_chain_start >= 2:
            df.loc[df['id1'] == list_shows[index], 'max_chain'] = max_chain_start
            teste = df[df['id1'] == list_shows[index]]

        
    #NOTE the index of the array and matrix are the node_id
    # make relations matrix and add max_chain to each
    matrix = np.zeros((len(df_list.index), len(df_list.index)), dtype = int)
    matrix[df["node_id1"].values, df["node_id2"].values] = df["max_chain"].values
    matrix_size = matrix.shape[0]
    #np array with the id and tag_id of each node
    indexs = df_list[["id", "tag_id"]].to_numpy(dtype = "int64")
    indexs_size = indexs.shape[0]   


    #make dict with the nodes of shows and tags
    #NOTE groupby(withou the extra operation like sum,mean etc) splits df into groups basically and for each group 
    # it has the id and the portion of the dataframe that has that id. 
    grouped_by_id = df_list.groupby("id")["node_id"]
    grouped_by_tag_id = df_list.groupby("tag_id")["node_id"]
    dic_id = {}
    dic_tag_id = {}
    matrix_out = np.zeros((indexs_size,indexs_size), dtype=bool)
    for i  in range(0,indexs_size):
        nodes_out = (df_list["id"] == df_list.iloc[i]["id"]) | (df_list["tag_id"] == df_list.iloc[i]["tag_id"])
        matrix_out[i,:]= nodes_out
    for id, group in grouped_by_id:
        mask = np.zeros(indexs_size, dtype=bool)
        mask[group.to_numpy()] = True
        dic_id[id] = mask
    for tag_id, group in grouped_by_tag_id:
        mask = np.zeros(indexs_size, dtype=bool)
        mask[group.to_numpy()] = True
        dic_tag_id[tag_id] = mask


    #prepare the chain list
    #TODO build a chain class to be used instead of list
    grouped_df = df_list.groupby("node_id")
    t1=time.time()
    chains_by_node = []

    for node_id, group in grouped_df:
        chains = []
        
        for _, row in group.iterrows():
            current_node_id = row["node_id"]
            current_id = row["id"]
            current_tag_id = row["tag_id"]

            # combined the nodes out because of id and tag_id
            #NOTE "|" is used to make union between sets
            combined_nodes_out = dic_tag_id.get(current_tag_id) | dic_id.get(current_id)

            chains.append((
                current_node_id,         # node_id of last element in the chain
                (current_id,),            # chain of id
                (current_tag_id,),        # chain of tag_id
                combined_nodes_out,    # list of unique node_ids
                1                        # elements in the chain
            ))
        
        chains_by_node.append([node_id, chains])
    t2=time.time()
    print(t2-t1)
    t1=time.time()


    #TODO this might be better for larger set because it doesn't loop the group (although zip might still required the lopp i don't know)
    chains_by_node2 = []
    for node_id, group in grouped_df:
        chains = list(
            zip(
                group["node_id"].values,                                                # node_id of last element in the chain
                [[id] for id in group["id"].values],                                     # Chain of ids
                [[tag_id] for tag_id in group["tag_id"].values],                        # Chain of tag_ids
                [np.union1d(dic_tag_id.get(tag_id) , dic_id.get(id))     # list of unique node_ids
                for tag_id, id in zip(group["tag_id"], group["id"])],
                [1] * len(group)                                                                      # elements in the chain
            )
        )
        chains_by_node2.append([node_id, chains])
    t2=time.time()
    print(t2-t1)


    #setting up shared variables to be used across process
    #NOTE both boolean_matrix_mask and indexs_shared_array are for read only operations as such I can send a copy to each process or shared them
    #across the process if i have memory problems
    boolean_matrix_mask = matrix > 0
    indexs_shared_array = multiprocessing.Array("i",indexs.flatten(), lock=False)
    matrix_shared_array = multiprocessing.Array('i', matrix.flatten(), lock=False)
    matrix_lock = multiprocessing.Lock()
    #TODO instead of using np.frombuffer with multiprocess.array it could be better to use 
    #multiprocess.shared_memory.SharedMemory although I would need to explicitly clear the allocated memory (WARNING!!!)
    #NOTE I have to use int32 because multiprocess.array doesn't support int64
    matrix = np.frombuffer(matrix_shared_array, dtype='int32').reshape(matrix_size, matrix_size)
    #TODO Queue seems to be better to get taks but to add them i would have to loop and add them individually while i can append a bunch if i use a list
    manager = multiprocessing.Manager()
    qlist = manager.list()
    task_counter = multiprocessing.Value("i", 0)
    max_shared = multiprocessing.Value("i", 0)
    chain_lock = multiprocessing.Lock()

    print("starts multiprocess aka problems")

    #Initialized the main_loop process,
    #main_loop will only build chains if it's possible to reach the wanted_chain
    #and stops chains if wanted_chain it can't be reached
    process = multiprocessing.Process(
        target=multiprocessing_main_loop.main_loop, 
        args=(chains_by_node, dic_id, dic_tag_id, wanted_chain, 
              matrix_shared_array, matrix_size, indexs_shared_array, indexs_size))
    process.start()
    # tracemalloc.start()
    # snapshot = tracemalloc.take_snapshot()
    # Run multiprocessing or target code
    profiler = cProfile.Profile()
    profiler.enable()

    t1 = time.time()
    #NOTE this process will loop every possible chain and updated the maxium chain of the node in the matrix
    with multiprocessing.Pool(
                processes = number_processes, initializer = multiprocessing_max_update.init_arr, 
                initargs = (qlist, boolean_matrix_mask,indexs,matrix_out, dic_id, dic_tag_id, 
                            task_counter, max_shared, chain_lock),
                maxtasksperchild = max_task_per_process) as pool:
        i=0

        for chains in chains_by_node:
            #snapshot1 = tracemalloc.take_snapshot()
            qlist.extend(chains[1])
            max_local = 0
            start_index = chains[0]
            task_counter.value = 0
            counter = 0
            node_id = chains[0]
            print("-node "+ str(chains[1][0][0]) +" - id " +str(chains[1][0][1][0])+ " - tag " + str(chains[1][0][2][0]))
            while True:
                # Check if there are tasks in the queue and available processes
                if (qlist) and (task_counter.value < number_processes):
                     #I had put this outside the child because when the counter was added inside the process
                    #the main process would have requests a bunch of new taks because "task_counter.value < number_processes"
                    with chain_lock:
                        chain = qlist.pop()
                    with task_counter.get_lock():
                        task_counter.value += 1
                    
                    if max_shared.value > max_local:
                        max_local = max_shared.value 
                    counter +=1
                    

                    if counter % 100 == 0:
                        print("node id - "+str(node_id)+" - id " +str(chains[1][0][1][0])+ " - tag " + str(chains[1][0][2][0]) +
                          " apply counter - " + str(counter)+" with task counter = " + str(task_counter.value) + " qlist len "+ str(len(qlist)))
                   

                    pool.apply_async(multiprocessing_max_update.max_update, args=(chain, number_processes, max_local), 
                                     error_callback = multiprocessing_max_update.error_callback)
                    
                elif (not qlist) and (task_counter.value ==  0):
                    print("maxium update stoped")
                    break
            
            # TODO I don't think i need the lock since the matrix is not updade in child process anymore
            with matrix_lock:
                matrix[:,start_index][matrix[:,start_index] > 0] = max_shared.value
                aux = matrix[:,start_index]
                print("max of node " + str(start_index) +" is " + str(max_shared.value))
                print("next")

            # Break for loop if main process is done but it still requires the pool the end the active node
            if not process.is_alive():
                print("main_loop_sto")  
                break

            # snapshot2 = tracemalloc.take_snapshot()
            # top_stats = snapshot2.compare_to(snapshot, 'lineno')
            # for stat in top_stats[:10]:
            #     print(stat)
            # print("next")
            
        print("pool is closing")
        pool.close()
        pool.join()
    print("pool closed")
    process.terminate()
    process.join()
    manager.shutdown()
    profiler.disable()
    sortby = pstats.SortKey.CUMULATIVE
    ps = pstats.Stats(profiler).sort_stats(sortby)
    #ps.print_stats()
    return None

