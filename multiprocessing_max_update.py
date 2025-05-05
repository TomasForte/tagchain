import numpy as np
from queue import Empty
import json
import gc
import time
import tracemalloc
import logging
from numba import jit
import cProfile
import pstats
from pstats import SortKey
import pstats
from pstats import SortKey


#NOTE THE error_callback as well as the callback arg of apply_asynch are run in the main process
#so task_counter is not accessible in this function 
#TODO find away to update task_count when function fail to keep number of task_corrected
def error_callback (e):
    try:
        logging.error(f"Error occurred: {e}")
        with task_counter.get_lock():  # Ensure thread-safe access
            task_counter.value -= 1
    except Exception as e:
        print(f"Error in error_callback: {e}")




def init_arr(task_stack, boolean_matrix_mask, nodes, matrix_out, task_counter, max_chain, chain_lock, batch_size, number_processes):
    #NOTE although task_stack is no a constant i decided to pass as global variable to avoid having to pass a big list between process 
    globals()['task_stack'] = task_stack
    globals()["task_counter"] = task_counter
    globals()["max_chain"] = max_chain
    globals()["chain_lock"] = chain_lock
    globals()["boolean_matrix_mask"] = boolean_matrix_mask
    globals()["nodes"] = nodes
    globals()["matrix_out"] = matrix_out
    globals()["batch_size"] = batch_size
    globals()["number_processes"] = number_processes
    # profiler = cProfile.Profile()
    # profiler.enable()
    # globals()["profiler"] = profiler 


#https://stackoverflow.com/questions/64222805/how-to-pass-2d-array-as-multiprocessing-array-to-multiprocessing-pool


def max_update(chain, current_max):
     
    try:
        global task_stack
        global chain_lock
        global boolean_matrix_mask
        global task_counter
        global max_chain
        global nodes
        global matrix_out
        global batch_size
        global number_processes
        global profiler
        # profiler = cProfile.Profile()
        # profiler.enable()
        counter = 1
        chains = [chain]
        local_max = current_max
        while chains:
            while counter < batch_size and chains:
                chain = chains.pop()
                chain_index = chain[0]
                id_chain =  chain[1]
                tag_id_chain =  chain[2]
                nodes_out =  chain[3]
                size = chain[4]
                #get the nodes where is possible to have a chain greater than the chain value
                #remove the nodes if is in the nodesout list
                #option1--------
                # connect_nodes = np.where(boolean_matrix_mask[chain_index,:])[0]
                # impossible_nodes_mask = np.isin(connect_nodes, nodes_out, assume_unique=True)
                # next_nodes = connect_nodes[~impossible_nodes_mask]
                #option2--------
                # connect_nodes = np.where(boolean_matrix_mask[chain_index,:])[0]
                # next_nodes = np.setdiff1d(connect_nodes, nodes_out, assume_unique=True)
                #option3--------
                # connect_nodes = np.where(boolean_matrix_mask[chain_index,:])[0]
                # indices = np.searchsorted(nodes_out, connect_nodes)
                # next_nodes = connect_nodes[((indices >= len(nodes_out)) | nodes_out[indices] != connect_nodes)]
                #ption4---------
                # connect_nodes = boolean_matrix_mask[chain_index,:]
                # impossible_nodes = np.zeros(connect_nodes.size, dtype=bool)
                # impossible_nodes[nodes_out] = True
                # next_nodes = np.where((connect_nodes == True) & (impossible_nodes == False))[0]
                #option5------
                connect_nodes = boolean_matrix_mask[chain_index,:]
                next_nodes = np.where(connect_nodes & ~nodes_out)[0]
                n_next_nodes = next_nodes.size
                counter += 1

                #check if there're possible nextnodes
                if n_next_nodes >= 1:
                    next_size = size + 1
                    if local_max < next_size:
                        local_max = next_size

                    #if there is more than one nextnode append to chains
                    #NOTE verse loop because to make it most recent last (if i add older last the list i'll grow too much)
                    # -------------
                    # for i in range(n_next_nodes - 1, -1, -1):
                    #     node = next_nodes[i]
                    #     chains.append((
                    #         node,
                    #         id_chain + (indexs[node][0],),
                    #         tag_id_chain + (indexs[node][1],),
                    #         nodes_out | matrix_out[node,:],
                    #         next_size))
                    # for node in reversed(next_nodes):
                    #     chains.append((
                    #         node,
                    #         id_chain + (indexs[node][0],),
                    #         tag_id_chain + (indexs[node][1],),
                    #         nodes_out | matrix_out[node,:],
                    #         next_size))
                    # chains_to_add =[(
                    #     next_nodes[i],
                    #     id_chain + (indexs[next_nodes[i]][0],),
                    #     tag_id_chain + (indexs[next_nodes[i]][1],),
                    #     nodes_out | matrix_out[next_nodes[i],:],
                    #     next_size) for i in range(n_next_nodes - 1, -1, -1)]    
                    # chains.extend(chains_to_add)

                    chains_to_add =[(
                        node,
                        id_chain + (nodes[node][0],),
                        tag_id_chain + (nodes[node][1],),
                        nodes_out | matrix_out[node,:],
                        next_size) for node in reversed(next_nodes)]   
                    chains.extend(chains_to_add)
                    #-------------------
                    # reversed_nodes = next_nodes[::-1]
                    # next_ids = indexs[reversed_nodes][:, 0]
                    # a_arr = np.asarray(id_chain)
                    # next_id_chain = np.column_stack([np.tile(a_arr, (len(next_ids), 1)), next_ids])

                    # next_tag_ids = indexs[reversed_nodes][:, 1]
                    # b_arr = np.asarray(tag_id_chain)
                    # next_tag_id_chain = np.column_stack([np.tile(b_arr, (len(next_tag_ids), 1)), next_tag_ids])

                    # next_nodes_out = np.logical_or(nodes_out, matrix_out[reversed_nodes,:])

                    # next_chain_size = [next_size] * n_next_nodes
                    # next_chains = list(zip(reversed_nodes, next_id_chain, next_tag_id_chain, next_nodes_out, next_chain_size))
                    # chains.extend(next_chains)

                    #------------------
                    # pass
                    #-------------------
                    # reversed_nodes = next_nodes[::-1]
                    # next_ids = indexs[reversed_nodes][:, 0]
                    # next_tag_ids = indexs[reversed_nodes][:, 1]
                    
                    
                    # new_chains = [
                    #     (
                    #         node, 
                    #         id_chain + (nid,),    
                    #         tag_id_chain + (ntag,),    
                    #         nodes_out | matrix_out[node,:],
                    #         next_size
                    #     )
                    #     for node, nid, ntag in zip(reversed_nodes, next_ids, next_tag_ids)
                    #     ]
                    # chains.extend(new_chains)
                    #-----------------
                    # reversed_nodes = next_nodes[::-1]
                    # precomputed_data = [(indexs[node][0], indexs[node][1], nodes_out | matrix_out[node, :]) for node in reversed_nodes]

                    # new_chains = [
                    #     (
                    #         node,
                    #         id_chain + (nid,),
                    #         tag_id_chain + (ntag,),
                    #         data,
                    #         next_size
                    #     )
                    #     for node, (nid, ntag, data) in zip(reversed_nodes, precomputed_data)
                    # ]

                    # chains.extend(new_chains)

            if chains:
                if len(chains) >= 1:
                    if task_counter.value <  number_processes:
                        with chain_lock:
                            if not task_stack:
                                chain = chains.pop(0)
                                task_stack.extend(chains)
                                chains = [chain]
                                counter = 0
                    else:
                        counter = 0
                #if there is only 1 chain there's no need to add it to the process
                else:
                    counter = 0
        # sortby = SortKey.CUMULATIVE
        # ps = pstats.Stats(profiler).sort_stats(sortby)
        # ps.print_stats(10) 


    except Exception as e:
        logging.error(f"Error in max_update: {e}")

    try:
        if local_max > current_max:
            with max_chain.get_lock():
                if local_max > max_chain.value:          
                    max_chain.value = local_max
    except Exception as e:
        logging.error(f"Error in updating max_chain: {e}")

    try:    
        with task_counter.get_lock():
            task_counter.value -= 1
    except Exception as e:
        logging.error(f"Error in updating task_counter: {e}")

    return None



    