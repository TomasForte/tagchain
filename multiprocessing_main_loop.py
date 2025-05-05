import numpy as np
import json
import logging
from numba import jit
import cProfile
from pstats import SortKey
import  pstats


#TODO maybe add add an arguement to make to allow the max_chain to get chains above the wanted_chain limit
#@jit(nopython=True)
def main_loop(chains_by_node, matrix_out, wanted_chain, matrix, matrix_size, nodes):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s  - %(message)s')
    matrix = np.frombuffer(matrix, dtype='int32').reshape(matrix_size, matrix_size)
    # profiler = cProfile.Profile()
    # profiler.enable()
    chains_found=[]
    max_chain = 1
    iteration_counter = 0
    try:
        for node in chains_by_node:
            logging.info("MAIN LOOP: starting node - " + str(node[0]) + ", id - " + str(node[1][0][1][0]))
            max_chain_by_id = 1
            chains=node[1]
            iteration_counter = 0
            found = False
            while chains:
                
                chain = chains.pop()
                node_id = chain[0]
                id_chain =  chain[1]
                tag_id_chain =  chain[2]
                nodes_out =  chain[3]
                chain_size = chain[4]
                #get the nodes where is possible to have a chain greater than the chain value
                connect_nodes = matrix[node_id,:] > (wanted_chain-chain_size)
                #get the nodes from the connect_nodes that are excluded from the chain
                next_nodes = np.where(connect_nodes & ~nodes_out)[0]
                n_next_nodes = next_nodes.size

                #check if there're possible nextnodes
                if n_next_nodes >= 1:
                    next_size = chain_size + 1
                    next_chain_size = chain_size + 1
                    if max_chain_by_id < next_chain_size:
                        max_chain_by_id = next_chain_size

                    #Add chain to chains_found if the size is greater than the specified size
                    if next_chain_size >= wanted_chain:
                        for i in range(0, n_next_nodes):
                            next_node = next_nodes[i]
                            next_id = nodes[next_node][0]
                            next_tag_id = nodes[next_node][1]
                            next_chain = [[*id_chain ,next_id],[*tag_id_chain, next_tag_id]]
                            chains_found.append(next_chain)
                            found = True
                        
                    else:
                        #add the chains to the stack of chains    
                        chains.extend([(
                            node,
                            id_chain + (nodes[node][0],),
                            tag_id_chain + (nodes[node][1],),
                            nodes_out | matrix_out[node,:],
                            next_size) for node in reversed(next_nodes)]
                        )





                    

                    # iteration_counter += 1
                    # **Print profiling stats every X iterations**
                    # if iteration_counter % 100000 == 0:
                    #     profiler.disable()
                    #     stats = pstats.Stats(profiler)
                    #     stats.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats(10)  # Print top 5 slowest functions
                    #     profiler.enable()
                    #     exit(1)

            logging.info("MAIN LOOP: concluded node_id - " + str(node[0])+ ", max - "+str(max_chain_by_id))
            if max_chain_by_id > max_chain:
                max_chain = max_chain_by_id
            
            #TODO: I don't need to save this every loop, I only need to save when the program stops so that I can resume it
            if found:
                #TODO load the chains from the previous run
                try: 
                    with open(r"previous_run\match.json", 'w') as output_file:
                        json.dump(chains_found, output_file, indent=2)
                except Exception as e:
                    #NOTE: since i don't clear the chains_found_list the chains will be added in the next loop
                    #TODO: Might be better to clear the chains_found list after saving to prevent the list from growing too much
                    # and not saving the chains_found every loop even when there are no new chains
                    # In this case it might be a better idea to brea the loop
                    logging.error("Error saving chains to file: " + str(e))
            
            # store the node id of the corrent run
            try:
                with open(r'previous_run\main_loop_node.bin', 'wb') as file:
                    file.write(node[0].to_bytes(32, byteorder='big'))
            except Exception as e:
                logging.error("Error saving the corrent node of MAIN LOOP" + str(e))
    
    except Exception as e:
        logging.error("Error in main loop: " + str(e))    