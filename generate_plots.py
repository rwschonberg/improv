import pickle
import matplotlib
from matplotlib import pyplot as plt
import math

with open("redis_logs.txt", "rb") as f:
    redis_logs = pickle.loads(f.read())
with open("plasma_logs.txt", "rb") as f:
    plasma_logs = pickle.loads(f.read())

##### Redis #####
x_count = [test[0] for test in redis_logs]
filtered_x_count = sorted(list(set(x_count)))
x_sizes = [test[1]*8 for test in redis_logs]
filtered_x_sizes = sorted(list(set(x_sizes)))

##### Plasma #####
x_count_plasma = [test[0] for test in plasma_logs]
filtered_x_count_plasma = sorted(list(set(x_count_plasma)))
x_sizes_plasma = [test[1]*8 for test in plasma_logs]
filtered_x_sizes_plasma = sorted(list(set(x_sizes_plasma)))

##### Fixed Count in Redis #####
y_redis = []
test_num = 0
test_count = redis_logs[0][0]
current_y = []
for count in filtered_x_count:
    for test in redis_logs:
        if test[0] == count:
            current_y.append(test[2])
    y_redis.append(current_y)
    current_y = []

fig, ax = plt.subplots(figsize=(9, 7))

for count_num in range(len(y_redis)):
    ax.plot(filtered_x_sizes[0:len(y_redis[count_num])],
            y_redis[count_num],
            label=f"{filtered_x_count[count_num]}",
            linewidth=2.5)

ax.set_xlabel("Message size (Bytes)", fontsize=15.0)
ax.set_ylabel("Time (sec)", fontsize=15.0)
ax.set_title('Time to Send Fixed Count of Messages in Redis', fontsize=18.0)
ax.set_xscale('log')
ax.set_yscale('log')
ax.set_ylim([1e-5, 1e2])
ax.legend(title="Number of Messages", fancybox=True)
plt.show()

##### Fixed Count in Plasma #####
y_plasma = []
test_num = 0
test_count = plasma_logs[0][0]
current_y = []
for count in filtered_x_count_plasma:
    for test in plasma_logs:
        if test[0] == count:
            current_y.append(test[2])
    y_plasma.append(current_y)
    current_y = []

fig, ax = plt.subplots(figsize=(9, 7))

for count_num in range(len(y_plasma)):
    ax.plot(filtered_x_sizes_plasma[0:len(y_plasma[count_num])],
            y_plasma[count_num],
            label=f"{filtered_x_count_plasma[count_num]}",
            linewidth=2.5)

ax.set_xlabel("Message size (Bytes)", fontsize=15.0)
ax.set_ylabel("Time (sec)", fontsize=15.0)
ax.set_title('Time to Send Fixed Count of Messages in Plasma', fontsize=18.0)
ax.set_xscale('log')
ax.set_yscale('log')
ax.set_ylim([1e-5, 1e2])
ax.legend(title="Number of Messages", fancybox=True)
plt.show()

##### Difference from Redis to Plasma #####

fig, ax = plt.subplots(figsize=(9, 7))

for count_num in range(len(y_plasma)):
    ax.plot(filtered_x_sizes_plasma[0:len(y_plasma[count_num])],
            [math.fabs(y_redis[count_num][i] - y_plasma[count_num][i]) for i in range(len(y_plasma[count_num]))],
            label=f"{filtered_x_count_plasma[count_num]}",
            linewidth=2.5)

ax.set_xlabel("Message size (Bytes)", fontsize=15.0)
ax.set_ylabel("Time: Absolute Value (sec)", fontsize=15.0)
ax.set_title('Absolute Difference In Fixed Message Count Time: Redis - Plasma', fontsize=18.0)
ax.set_xscale('log')
ax.set_yscale('log')
ax.legend(title="Number of Messages", fancybox=True)
plt.show()

##### Speedup from Plasma to Redis #####

fig, ax = plt.subplots(figsize=(9, 7))

for count_num in range(len(y_plasma)):
    ax.plot(filtered_x_sizes_plasma[0:len(y_plasma[count_num])],
            [y_plasma[count_num][i]/y_redis[count_num][i] for i in range(len(y_plasma[count_num]))],
            label=f"{filtered_x_count_plasma[count_num]}",
            linewidth=2.5)

ax.set_xlabel("Message size (Bytes)", fontsize=15.0)
ax.set_ylabel("Speedup", fontsize=15.0)
ax.set_title('Speedup of Redis Over Plasma for Fixed Message Count', fontsize=18.0)
ax.set_xscale('log')
ax.set_ylim([0, 3.0])
ax.legend(title="Number of Messages", fancybox=True)
plt.show()

##### Fixed Size in Redis #####
y_redis = []
test_num = 0
test_size = redis_logs[0][1]
current_y = []
for size in filtered_x_sizes:
    for test in redis_logs:
        if test[1]*8 == size:
            current_y.append(test[2])
    y_redis.append(current_y)
    current_y = []

fig, ax = plt.subplots(figsize=(9, 7))

for size_num in range(len(y_redis)):
    ax.plot(filtered_x_count[0:len(y_redis[size_num])],
            y_redis[size_num],
            label=f"{filtered_x_sizes[size_num]}B",
            linewidth=2.5)

ax.set_xlabel("Message Count", fontsize=15.0)
ax.set_ylabel("Time (sec)", fontsize=15.0)
ax.set_title('Time to Send Messages of Fixed Size in Redis', fontsize=18.0)
ax.set_xscale('log')
ax.set_yscale('log')
ax.set_ylim([1e-5, 1e2])
ax.legend(title="Message Size", fancybox=True)
plt.show()


##### Fixed Size in Plasma #####
y_plasma = []
test_num = 0
test_size = plasma_logs[0][1]
current_y = []
for size in filtered_x_sizes_plasma:
    for test in plasma_logs:
        if test[1]*8 == size:
            current_y.append(test[2])
    y_plasma.append(current_y)
    current_y = []

fig, ax = plt.subplots(figsize=(9, 7))

for size_num in range(len(y_plasma)):
    ax.plot(filtered_x_count_plasma[0:len(y_plasma[size_num])],
            y_plasma[size_num],
            label=f"{filtered_x_sizes_plasma[size_num]}B",
            linewidth=2.5)

ax.set_xlabel("Message Count", fontsize=15.0)
ax.set_ylabel("Time (sec)", fontsize=15.0)
ax.set_title('Time to Send Messages of Fixed Size in Plasma', fontsize=18.0)
ax.set_xscale('log')
ax.set_yscale('log')
ax.set_ylim([1e-5, 1e2])
ax.legend(title="Message Size", fancybox=True)
plt.show()

##### Difference Plasma to Redis #####

fig, ax = plt.subplots(figsize=(9, 7))

for size_num in range(len(y_plasma)):
    ax.plot(filtered_x_count_plasma[0:len(y_plasma[size_num])],
            [math.fabs(y_redis[size_num][i] - y_plasma[size_num][i]) for i in range(len(y_plasma[size_num]))],
            label=f"{filtered_x_sizes_plasma[size_num]}B",
            linewidth=2.5)

ax.set_xlabel("Message Count", fontsize=15.0)
ax.set_ylabel("Time: Absolute Value (sec)", fontsize=15.0)
ax.set_title('Absolute Difference In Fixed Message Size Time: Redis - Plasma', fontsize=18.0)
ax.set_xscale('log')
ax.set_yscale('log')
ax.legend(title="Message Size", fancybox=True)
plt.show()

##### Speedup from Plasma to Redis #####

fig, ax = plt.subplots(figsize=(9, 7))

for size_num in range(len(y_plasma)):
    ax.plot(filtered_x_count_plasma[0:len(y_plasma[size_num])],
            [y_plasma[size_num][i]/y_redis[size_num][i] for i in range(len(y_plasma[size_num]))],
            label=f"{filtered_x_sizes_plasma[size_num]}B",
            linewidth=2.5)

ax.set_xlabel("Message Count", fontsize=15.0)
ax.set_ylabel("Speedup", fontsize=15.0)
ax.set_title('Speedup of Redis over Plasma for Fixed Message Size', fontsize=18.0)
ax.set_xscale('log')
ax.set_ylim([0, 3.0])
ax.legend(title="Message Size", fancybox=True)
plt.show()