import numpy as np


import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt


t1 = 1
t2 = 2
t3 = 3

objects = ("RDD", "SQL/csv", "SQL/parquet")
y_pos = np.arange(len(objects))
performance = [t1,t2,t3]
plt.barh(y_pos, performance, align="center", alpha=0.5)
plt.yticks(y_pos,objects)
plt.xlabel("Time(s)")
plt.title("Time needed for Query 2")
plt.show()
plt.savefig("Q2.png")