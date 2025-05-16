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


#NOTE THE error_callback as well as the callback arg of apply_asynch are run in the main process not the child
#so task_counter is not accessible in this function 
#TODO find away to update task_count when function fail to keep number of task_corrected
def error_callback (e):

    logging.error(f"Error in error_callback: {e}")
    exit(1)




def init_arr(task_stack, boolean_matrix_mask, nodes, shared_matrix_array, matrix_out, task_counter, max_chain, chain_lock, batch_size, number_processes):
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
    shared_matrix = np.frombuffer(shared_matrix_array, dtype='int32').reshape(matrix_out.shape)
    globals()["shared_matrix"] = shared_matrix
    # profiler = cProfile.Profile()
    # profiler.enable()
    # globals()["profiler"] = profiler 


#https://stackoverflow.com/questions/64222805/how-to-pass-2d-array-as-multiprocessing-array-to-multiprocessing-pool


def max_update(process_number):
     
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
        global shared_matrix 
        test = shared_matrix > 0
        # profiler = cProfile.Profile()
        # profiler.enable()
        counter = 1
        local_max = 1
        chains = []
        while True:
            if (task_stack):
                #check again if empty because some other process my pop the last item between the first check and i don't want to add
                # a lock in a while True
                try:
                    
                    chains = [task_stack.pop()]
                    print("process " + str(process_number) + " started")
                    task_counter.value += 1
                except:
                    logging.debug("stack is became empty")
                    
                # don't lock if pop failed
                if chains:
                    if max_chain.value > local_max :      
                            local_max = max_chain.value

                while chains:
                    while counter < batch_size and chains:
                        chain = chains.pop()
                        chain_index = chain[0]
                        id_chain =  chain[1]
                        tag_id_chain =  chain[2]
                        nodes_out =  chain[3]
                        size = chain[4]
                        #get the nodes where is possible to have a chain greater than the chain value

                        threshold = local_max - size
                        mask = (shared_matrix[chain_index,:] > threshold) & (~nodes_out)
                        next_nodes = mask.nonzero()[0]
                        n_next_nodes = next_nodes.size
                        counter += 1

                        #check if there're possible nextnodes
                        if n_next_nodes >= 1:
                            next_size = size + 1
                            if next_size > local_max:
                                local_max = next_size


                            chains_to_add =[(
                                node,
                                id_chain + (nodes[node][0],),
                                tag_id_chain + (nodes[node][1],),
                                nodes_out | matrix_out[node,:],
                                next_size) for node in next_nodes]   
                            chains.extend(chains_to_add)

                    if local_max > max_chain.value:
                        with max_chain.get_lock():
                            if local_max > max_chain.value:          
                                max_chain.value = local_max
                    else:
                        local_max = max_chain.value

                    if chains:
                        if len(chains) >= 1:
                            if task_counter.value <  number_processes:
                                if not task_stack:
                                    chain = chains.pop()
                                    task_stack.extend(chains)
                                    chains = [chain]
                                    print("process " + str(process_number) + " filled stack")

                        counter = 0

                    else:

                        if task_stack:
                            try:
                                chains = [task_stack.pop()]
                                print("resetting process " + str(process_number))
                            except:
                                pass
                        if not chains:
                            with task_counter.get_lock():
                                task_counter.value -= 1
                            print("process " + str(process_number) + " stopped")

                
            if task_counter.value == 0:
                break

            # sortby = SortKey.CUMULATIVE
            # ps = pstats.Stats(profiler).sort_stats(sortby)
            # ps.print_stats(10) 


    except Exception as e:
        logging.error(f"Error in max_update: {e}")



    return None



    