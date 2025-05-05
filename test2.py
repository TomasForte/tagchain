from numba import jit
import numpy as np
import time
import multiprocessing



@jit(nopython=True)
def gogo_fast(a):
    trace = 0.0
    for i in range(a.shape[0]):
        trace += np.tanh(a[i, i])
    return trace


def go_fast(a):
    a = np.frombuffer(a, dtype='int32').reshape(10, 10) # Function is compiled and runs in machine code
    trace = gogo_fast(a)
    print("bob")
    return a + trace



if __name__ == "__main__":
    x = np.arange(100).reshape(10, 10)
    x = multiprocessing.Array('i', x.flatten(), lock=False)
    # DO NOT REPORT THIS... COMPILATION TIME IS INCLUDED IN THE EXECUTION TIME!
    start = time.perf_counter()
    go_fast(x)
    end = time.perf_counter()
    print("Elapsed (with compilation) = {}s".format((end - start)))

    # NOW THE FUNCTION IS COMPILED, RE-TIME IT EXECUTING FROM CACHE
    start = time.perf_counter()
    go_fast(x)
    end = time.perf_counter()
    print("Elapsed (after compilation) = {}s".format((end - start)))


    start = time.perf_counter()
    process = multiprocessing.Process(target=go_fast, args=(x,))
    process.start()
    process.join()
    end = time.perf_counter()
    print("Elapsed (after compilation) = {}s".format((end - start)))
