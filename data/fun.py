import numpy as np
from scipy.interpolate import interp1d

x = np.arange(0,24)
y = np.array([12,13,16,16,14,14,17,16,18,17,20,23,24,24,23,25,25,23,22,20,16,16,15,14])
fun = interp1d(x=x, y=y, kind=2,fill_value="extrapolate")