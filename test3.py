import numpy as np

matrix = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
print("Before change:\n", matrix)

matrix[:, 1] = 5  # Modify column 1
print("After change:\n", matrix)



import numpy as np

# Create a sample matrix
matrix = np.array([
    [1, 2, 3],
    [4, 5, 6],
    [7, 8, 9]
])

# Define the column index
i = 1



# Apply the condition to the i-th column
matrix[:, i][matrix[:, i] > 1] = 5

print("Original matrix:")
print(matrix)


# Sample shared_matrix
shared_matrix = np.array([
    [0, 2, 3],
    [4, 5, 6],
    [7, 8, 9]
])

# Define start_node and max_chain_shared.value
start_node = 1
max_chain_shared = 10 # Creating an object with a 'value' attribute

# Apply the operation
shared_matrix[:, start_node][shared_matrix[:, start_node] > 0] = 10

print("Updated matrix:")
print(shared_matrix)
