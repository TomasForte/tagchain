import numpy as np
i = 660
change = "max"
matrix = np.load(r"previous_run\array.npy")
np.savetxt("converted_matrix.csv", matrix, delimiter=",", fmt="%.5f")
max_value = np.max(matrix[:500, :])
result = np.argwhere(matrix == max_value)
print("OK")

connection_index = 500
with open(r'previous_run\max_update_conecting_node.bin', 'wb') as file:
    file.write(connection_index.to_bytes(32, byteorder='big'))
# if change == "mdasdasdain":
#     with open(r'previous_run\main_loop_node.bin', 'wb') as file:
#                         file.write(i.to_bytes(32, byteorder='big'))

# if change == "madasdasdax":
#     with open(r'previous_run\max_update_node', 'wb') as file:
#                         file.write(i.to_bytes(32, byteorder='big'))