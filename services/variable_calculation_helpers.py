import pandas as pd
import numpy as np

def epsilon(ncx, nc, nx, N):
    num = nx * ((ncx / nx) - (nc / N))
    den = np.sqrt(nx * (nc / N) * (1 - (nc / N)))
    return np.round(num / den, 2)

def log_lift(ncx, nc, nx, N):
    # num = (ncx / nx)
    # den = (nc / N)
    # return np.round(np.log(num / den), 2)
    return np.round(np.log((ncx / nx) / (nc / N)), 3)

def score(ncx, nc, nx, N):
    px_c  = (ncx + 0.005) / (nc + 0.01)
    px_nc = (nx - ncx + 0.01) / (N - nc + 0.005)
    return np.round(np.log(px_c / px_nc), 2)

def apply_haldane_if_needed(a, b, c, d):
    """
    Apply Haldane Anscombe (0.5)
    ONLY if there exists at least a 0 in the 2x2 table
    """
    there_is_zero = (a == 0) | (b == 0) | (c == 0) | (d == 0)
    if there_is_zero:
        a += 0.5
        b += 0.5
        c += 0.5
        d += 0.5
    return a, b, c, d