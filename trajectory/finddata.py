from numpy import genfromtxt
import os
from shutil import copyfile

left = 121.237639
right = 121.685351
top = 31.369900
bottom = 31.049123


files = os.listdir('./data')
# print(files)

for file in files:
    mydata = genfromtxt('./data/' + file, delimiter=',')

    # mydata[1][3] = 31.35
    # mydata[1][4] = 121.25

    if (mydata[1][3] > bottom and mydata[1][3] < top and mydata[1][4] > left and mydata[1][4] < right):
        print('true:{}'.format(file))
        copyfile('./data/' + file, './sh_data/' + file)
    else:
        print('flase')
