import pandas as pd
import numpy as np

def epsilon(ncx, nc, nx, N):
    num = nx * ((ncx / nx) - (nc / N))
    den = np.sqrt(nx * (nc / N) * (1 - (nc / N)))
    return np.round(num / den, 2)

def log_lift(ncx, nc, nx, N):
    num = (ncx / nx)
    den = (nc / N)
    return np.round(np.log(num / den), 2)

def score(ncx, nc, nx, N):
    px_c  = (ncx + 0.005) / (nc + 0.01)
    px_nc = (nx - ncx + 0.01) / (N - nc + 0.005)
    return np.round(np.log(px_c / px_nc), 2)