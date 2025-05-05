import argparse
import pandas as pd
import database
import setup
import logging
import child_handling
import os
import numpy as np

def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog='PROG',
        description='''Get a chain of movie tags 
        RULES: 
        1. pick movie and a tag 
        2. pick movie with the previous tag and select another tag 
        3. Repeat 2 until you get a chain of the size stated 
        Conditions:
        1. You can't pick movies with finished date before the last movie in the chain, same date is ok
        2. Tags and movies can repeat in the chain.''',
        epilog="Let's hope it works!!!"
    )

    parser.add_argument("-t", "--test", 
                        action="store_true", 
                        help="Use sample Excel data for testing.")
    parser.add_argument("-s", "--wantedchainsize", 
                        type=int, default=40, 
                        help="Size of the chain.", metavar=" ")
    parser.add_argument("-p", "--processes", 
                        type=int, default=4, 
                        help="Number of processes for updating chains.", metavar=" ")
    parser.add_argument("-b", "--batchsize", 
                        type=int, default=10000, 
                        help="Batch size for processing chains (not implemented).", metavar=" ")
    parser.add_argument("-m", "--maxtaskperprocess", 
                        type=int, default=10000, 
                        help="Max tasks per child process before restarting.")
    parser.add_argument("-c", "--continuepreviousrun", 
                        action="store_true", 
                        help="continue where the last run left off")
    args = parser.parse_args()

    if args.wantedchainsize <= 2:
        parser.error("The chain size must be greater than 2.")
    if args.processes <= 0:
        parser.error("The number of processes must be greater than 0.")
    if args.batchsize <= 0:
        parser.error("The batch size must be greater than 0.")

    return args

def validate_previous_run(folder_path, continue_previous_run):
    """Validate content of the previous run folder based on the continue_previous_run flag."""
    if not os.listdir(folder_path):
        if continue_previous_run:
            logging.error(f"The {folder_path} directory is empty. Can't continue previous run.")
            exit(1)
    else:
        if not continue_previous_run:
            logging.error(f"The {folder_path} directory is not empty. Can't start a new run.")
            exit(1)


def load_from_excel():
    """Load test data from sample Excel file."""
    #TODO fix column and what not to make it work
    path = r"sample.ods"
    try:
        df_tags = pd.read_excel(path, sheet_name='Tags')
        df = pd.read_excel(path, sheet_name='movies')
    except FileNotFoundError:
        logging.error(f"Excel file not found at {path}.")
        exit(1)
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        exit(1)    

    df_tags = df_tags[df_tags['id'].notna()]
    df_tags['tag_id'] = df_tags['tag_id'].astype('int32')


    df = df[df['id'].notna()]
    df['tag_id'] = df['tag_id'].astype('int32')

    return df, df_tags


def load_from_db():
    """Load production data from the database and Excel file."""
    try:
        db = database.challengedb()
    except Exception as e:
        logging.error(f"Error connecting to db: {e}")
        exit(1)
    try:
        df = db.get_movies()
    except Exception as e:
        logging.error(f"Error query data from db: {e}")
        exit(1)
    
    path = r"C:\Users\Utilizador\Desktop\badges\helper-excel\AP-Tags.ods"
    try:
        df_tags = pd.read_excel(path, sheet_name='tagChain')
    except FileNotFoundError:
        logging.error(f"Excel file not found at {path}.")
        exit(1)
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        exit(1)

    df_tags = df_tags[df_tags['id'].notna()]
    df_tags['tag_id'] = df_tags['tag_id'].astype('int32')

    return df, df_tags

def resume_previous_run(folder_path, chains_by_node, nodes_size, matrix_original):
    """Load data from the previous run."""

    # load relations matrix from previous run
    try:
            #NOTE: Overwirting the matrix with the one from the previous run
            matrix = np.load(r'previous_run\array.npy')
                        # if the matrix from the previous run is not the same size as the loaded data, exi
    except FileNotFoundError:
        logging.error("Relations matrix not found.")
        exit(1)

    # Load the last node of the max_update loop
    try:
        with open(r'previous_run\max_update_node.bin', 'rb') as file:
                start_max_update_node = int.from_bytes(file.read(32), byteorder='big')
    except FileNotFoundError:
        logging.error("max_update node not found")
        exit(1)

    # Load the last node of the main_loop loop
    try:
            with open(r'previous_run\main_loop_node.bin', 'rb') as file:
                start_main_loop_node = int.from_bytes(file.read(32), byteorder='big')
    except FileNotFoundError:
        logging.error("main_loop node not found")
        exit(1)


    if nodes_size != matrix.shape[0]:
        logging.error("Matrix from previous run is not the same size as the loaded data.")
        exit(1)
    elif not np.array_equal(matrix > 0, matrix_original > 0):
        logging.error("Matrix from previous doesn't match the loaded data.")
        exit(1)
    matrix = np.hstack((matrix[:,:start_max_update_node], matrix_original[:,start_max_update_node:]))
    # np.savetxt("matrix.csv", matrix,  
    #   delimiter = ",")
    # np.savetxt("matrix_org.csv", matrix_original,  
    #   delimiter = ",")


    # Update the list of chains to be ran by each child process based on the starting node of the previous run
    if start_main_loop_node > 0:

        #NOTE to self: next() return the first element of an iterator
        #itetor is build using a generator expression (basically a list comprehension with paranthesis instead of brackets)
        #if the iterator is empty it will raise a StopIteration exception
        index_main_loop = next((i for i, chain in enumerate(chains_by_node) if chain[0] == start_main_loop_node), None)
        if index_main_loop is not None:
            chains_by_node_main_loop = chains_by_node[index_main_loop + 1:]
            logging.info("Starting main_loop from node " + str(chains_by_node_main_loop[0][0]) + ", id - " + str(chains_by_node_main_loop[0][1][0][1][0]))
        else:
            logging.error("Starting node of main_loop not found in chains_by_node.")
            exit(1)
    else:
        chains_by_node_main_loop = chains_by_node

    if start_max_update_node > 0:
        index_max_update = next((i for i, chain in enumerate(chains_by_node) if chain[0] == start_max_update_node), None)
        if index_max_update is not None:
            chains_by_node_max_update = chains_by_node[index_max_update + 1:]
            logging.info("Starting max_update from node " + str(chains_by_node_max_update[0][0]) + ", id - " + str(chains_by_node_max_update[0][1][0][1][0]))
        else:
            logging.error("Starting node of max_update not found in chains_by_node.")
            exit(1)
    else:
        chains_by_node_max_update = chains_by_node

    return matrix, chains_by_node_main_loop, chains_by_node_max_update



def main():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s  - %(message)s')
    logging.info("Starting the script...")
    
    #parse the arguments
    args = parse_arguments()

    # Validate previous run folder 
    folder_path = "previous_run"
    os.makedirs(folder_path, exist_ok=True)
    validate_previous_run(folder_path, args.continuepreviousrun)
    

    logging.info("Loading data...")
    if args.test:
        df_movies, df_tags = load_from_excel()
    else:
        df_movies, df_tags = load_from_db()


 
    logging.info("Preparing data...")
    df, df_nodes = setup.prepare_data(df_movies, df_tags)
    logging.info("Building relations...")
    df_relations = setup.build_relations_dataframe(df, df_nodes)
    logging.info("Setting up relations matrix...")
    nodes, nodes_size, matrix, matrix_out = setup.setup_shared_variables(df_relations, df_nodes)
    chains_by_node = setup.build_chain_list(df_nodes, matrix_out)


    #NOTE: the chains_by_node is a list of tuples, where each tuple is a node and a list of chains
    #Load data from the previous run if the continue_previous_run flag is set
    if args.continuepreviousrun:
        logging.info("Load data from previous run...")
        matrix, chains_by_node_main_loop, chains_by_node_max_update = resume_previous_run(folder_path, chains_by_node, nodes_size, matrix)
    else:
        chains_by_node_main_loop = chains_by_node
        chains_by_node_max_update = chains_by_node

    
    setting = {"wanted_chain": args.wantedchainsize,
            "number_processes": args.processes,
            "batch_size": args.batchsize,
            "max_task_per_process": args.maxtaskperprocess}



    logging.info("Starting multiprossesses aka problems...") 
    child_handling.starting_child(
        nodes, nodes_size, matrix, matrix_out, chains_by_node_main_loop, chains_by_node_max_update, setting)


if __name__ == "__main__":
    main() 