import time
import numpy as np

import numpy as np

# Example matrices (same size)
A = np.array([[1, 2, 3, 4], [5, 6, 7, 8]])
B = np.array([[9, 10, 11, 12], [13, 14, 15, 16]])

# Define the split point (X)
X = 2  # Column index where the split happens

# Create the new matrix
new_matrix = np.hstack((A[:, :X], B[:, X:]))

print("New Matrix:")
print(new_matrix)
