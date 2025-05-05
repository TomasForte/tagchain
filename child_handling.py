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


   
       
def start_multiprocesses(chains_by_node_main_loop, chains_by_node_max_update, nodes_size, nodes, matrix_out, settings, shared_vars):   
    """start the main loop to build and pool to update the max chain of nodes in shared_matrix"""
    
    (boolean_matrix_mask, matrix_shared_array, task_stack,
            task_counter, max_chain_shared, matrix_lock, chain_lock) = shared_vars

    #NOTE main_loop will only build chains if it's possible to reach the wanted_chain
    process = multiprocessing.Process(
        target=multiprocessing_main_loop.main_loop, 
        args=(chains_by_node_main_loop, matrix_out, settings["wanted_chain"], 
              matrix_shared_array, nodes_size, nodes))
    try:
        process.start()
        logging.info("Main loop process started.")
    except Exception as e:
        logging.error(f"Failed to start main loop process: {e}")
        return

    
    shared_matrix = np.frombuffer(matrix_shared_array, dtype='int32').reshape(nodes_size, nodes_size)

    profiler = cProfile.Profile()
    profiler.enable()
    t1 = time.time()

    logging.info("Max update process started.")

    #NOTE this process will loop every possible chain and updated the maxium chain of the node in the matrix
    with multiprocessing.Pool(
                processes = settings["number_processes"], initializer = multiprocessing_max_update.init_arr, 
                initargs = (task_stack, boolean_matrix_mask,nodes, matrix_shared_array, matrix_out,
                            task_counter, max_chain_shared, chain_lock, settings["batch_size"], settings["number_processes"]),
                maxtasksperchild = settings["max_task_per_process"]) as pool:
        i=0

        for chains in chains_by_node_max_update:
            max_chain_shared.value = 1
            task_stack.extend(chains[1])
            max_local = 1
            start_node = chains[0]
            task_counter.value = 0
            counter = 0
            node_id = chains[0]
            logging.info("MAX UPDATE LOOP: starting node - "+ str(chains[1][0][0]) +", id - " +str(chains[1][0][1][0]))

            try:
                #TODO
                while True:
                    # Check if there are tasks in the stack and available processes
                    if (task_stack) and (task_counter.value < settings["number_processes"]):
                        #I had put this outside the child because when the counter was added inside the process
                        #the main process would have requests a bunch of new taks because "task_counter.value < number_processes"
                        with chain_lock:
                            chain = task_stack.pop()
                        with task_counter.get_lock():
                            task_counter.value += 1
                        
                        if max_chain_shared.value > max_local:
                            max_local = max_chain_shared.value 
                        counter +=1
                        

                        if counter % 100 == 0:
                            logging.info("MAX UPDATE LOOP: status node id - "+str(node_id)+", id - " +str(chains[1][0][1][0])+
                            ", counter - " + str(counter)+", task counter - " + str(task_counter.value) + ", tasks - "+ str(len(task_stack)))
                    

                        pool.apply_async(multiprocessing_max_update.max_update, args=(chain, max_local), 
                                        error_callback = multiprocessing_max_update.error_callback)
                        
                    elif (not task_stack) and (task_counter.value ==  0):
                        # If there are no tasks in the queue and all processes are done, break the loop
                        break
            except Exception as e:
                logging.error(f"Program stop: {e}")
                pool.terminate()
                pool.join()
                break

            
                        
            # Update the the max of the node in the shared matrix and save the matrix
            if max_chain_shared.value > 1:
                with matrix_lock:
                    try:
                        print("before " + str(shared_matrix[:,start_node].max() ))
                        print("after " + str(max_chain_shared.value))
                        old_max = shared_matrix[:,start_node].max()
                        #the old > 0 bit is because if there no other node that conects to it max was not calculated
                        # tecnically I don't even need to update the max of this node the no other node that connects to it
                        if (old_max < max_chain_shared.value) & (old_max > 0):
                            logging.error("start relations matrix setup logic is incorrect")
                            exit(1)
                        shared_matrix[shared_matrix[:, start_node] > 0, start_node] = max_chain_shared.value
                        
                        aux = shared_matrix[:,start_node]
                        logging.info("MAX UPDATE LOOP: concluded node " + str(start_node) +", max " + str(max_chain_shared.value))
                        with open(r'previous_run\array.npy', 'wb') as file:    
                            np.save(r'previous_run\array.npy', shared_matrix)

                        with open(r'previous_run\max_update_node.bin', 'wb') as file:
                            file.write(start_node.to_bytes(32, byteorder='big'))
                    except Exception as e:
                        #NOTE
                        logging.error(f"Error saving the matrix: {e}")



            if start_node >= 1040:
                t2 = time.time()
                print(t2-t1)
                break



            #Break for loop if main process is done
            if not process.is_alive():
                print("main_loop_sto")  
                break

           
        print("pool is closing")
        pool.close()
        pool.join()
    process.terminate()
    process.join()




def starting_child(nodes, nodes_size, matrix, matrix_out, chains_by_node_main_loop, chains_by_node_max_update, settings):
    """Start child processes to handle chains."""

    #setting up shared variables to be used across process
    boolean_matrix_mask = matrix > 0
    matrix_shared_array = multiprocessing.Array('i', matrix.flatten(), lock=False)
    
    #TODO instead of using np.frombuffer with multiprocess.array it could be better to use 
    #multiprocess.shared_memory.SharedMemory although I would need to explicitly clear the allocated memory (WARNING!!!)
    #NOTE I have to use int32 because multiprocess.array doesn't support int64
    shared_matrix = np.frombuffer(matrix_shared_array, dtype='int32').reshape(nodes_size, nodes_size)
    #TODO Queue seems to be better to get taks but to add them i would have to loop and add them individually while i can append a bunch if i use a list
    manager = multiprocessing.Manager()
    task_stack = manager.list()
    task_counter = multiprocessing.Value("i", 0)
    max_chain_shared = multiprocessing.Value("i", 0)

    chain_lock = multiprocessing.Lock()
    matrix_lock = multiprocessing.Lock()


    shared_vars = (boolean_matrix_mask, matrix_shared_array, task_stack,
                    task_counter, max_chain_shared, matrix_lock, chain_lock)

    start_multiprocesses(chains_by_node_main_loop, chains_by_node_max_update, nodes_size, nodes, matrix_out, settings, shared_vars)

    manager.shutdown()
