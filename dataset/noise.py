from datetime import datetime, timedelta
import random


fw = open("E:\\projects\\iotdb\\dataset\\gen.noise\\noise.csv", "w")

data_size = 10000000
col_size = 4

head = "Time,root.gen.noise.n1,root.gen.noise.n2,root.gen.noise.n3,root.gen.noise.n4\n"

fw.write(head)

now = datetime.now()

for i in range(data_size):
    if i%100000 == 0: 
        print("percentage: " + str(i/100000) + "/100")
    ts = now + timedelta(seconds=i*5)
    entry = str(ts)
    for j in range(col_size):
        entry += "," + str(random.random())
    fw.write(entry + "\n")

fw.close()