import numpy as np

# Sample matrix
matrix = np.array([
    [1, 2, 3],
    [4, 5, 6],
    [7, 8, 9]
])

# Define the column index
i = 1

# Expression 1 (does not modify the original matrix)
matrix[:, i][matrix[:, i] > 1] = 5
print("Matrix after Expression 1:")
print(matrix)

# Reset the matrix
matrix = np.array([
    [1, 2, 3],
    [4, 5, 6],
    [7, 8, 9]
])

# Expression 2 (modifies the original matrix)
matrix[matrix[:, i] > 1, i] = 5
print("Matrix after Expression 2:")
print(matrix)
