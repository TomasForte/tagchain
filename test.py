import time
import numpy as np

with open(r'previous_run\max_update_node.bin', 'rb') as file:
    start_max_update_node = int.from_bytes(file.read(32), byteorder='big')
with open(r'previous_run\main_loop_node.bin', 'rb') as file:
    start_main_loop_node = int.from_bytes(file.read(32), byteorder='big')
teste = 23
