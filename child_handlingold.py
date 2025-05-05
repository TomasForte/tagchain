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



def starting_child(nodes, nodes_size, matrix, matrix_out, dic_id, dic_tag_id, chains_by_node,
                    wanted_chain, number_processes, batch_size, max_task_per_process):
    """Start child processes to handle chains."""

    #setting up shared variables to be used across process
    #NOTE both boolean_matrix_mask and indexs_shared_array are for read only operations as such I can send a copy to each process or shared them
    #across the process if i have memory problems
    boolean_matrix_mask = matrix > 0
    matrix_shared_array = multiprocessing.Array('i', matrix.flatten(), lock=False)
    matrix_lock = multiprocessing.Lock()
    #TODO instead of using np.frombuffer with multiprocess.array it could be better to use 
    #multiprocess.shared_memory.SharedMemory although I would need to explicitly clear the allocated memory (WARNING!!!)
    #NOTE I have to use int32 because multiprocess.array doesn't support int64
    matrix = np.frombuffer(matrix_shared_array, dtype='int32').reshape(nodes_size, nodes_size)
    #TODO Queue seems to be better to get taks but to add them i would have to loop and add them individually while i can append a bunch if i use a list
    manager = multiprocessing.Manager()
    qlist = manager.list()
    task_counter = multiprocessing.Value("i", 0)
    max_chain_shared = multiprocessing.Value("i", 0)
    chain_lock = multiprocessing.Lock()

    print("starts multiprocess aka problems")

    #Initialized the main_loop process,
    #main_loop will only build chains if it's possible to reach the wanted_chain
    #and stops chains if wanted_chain it can't be reached
    process = multiprocessing.Process(
        target=multiprocessing_main_loop.main_loop, 
        args=(chains_by_node, dic_id, dic_tag_id, wanted_chain, 
              matrix_shared_array, nodes_size, nodes))
    try:
        process.start()
    except Exception as e:
        logging.error(f"Failed to start main loop process: {e}")
        return
    
    # tracemalloc.start()
    # snapshot = tracemalloc.take_snapshot()
    # Run multiprocessing or target code
    profiler = cProfile.Profile()
    profiler.enable()

    t1 = time.time()
    #NOTE this process will loop every possible chain and updated the maxium chain of the node in the matrix
    with multiprocessing.Pool(
                processes = number_processes, initializer = multiprocessing_max_update.init_arr, 
                initargs = (qlist, boolean_matrix_mask,nodes,matrix_out, dic_id, dic_tag_id, 
                            task_counter, max_chain_shared, chain_lock),
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
                    
                    if max_chain_shared.value > max_local:
                        max_local = max_chain_shared.value 
                    counter +=1
                    

                    if counter % 1 == 0:
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