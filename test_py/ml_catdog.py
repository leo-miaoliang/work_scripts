# -*- coding: utf-8 -*-

import numpy as np
import matplotlib.pyplot as plt
import os
import cv2
from tqdm import tqdm

DATADIR = "/root/ml_project/PetImages/" # 数据集的路径，请根据需要修改

CATEGORIES = ["Dog", "Cat"]
print('test')
for category in CATEGORIES:
    path = os.path.join(DATADIR,category)  # 创建路径
    for img in os.listdir(path):  # 迭代遍历每个图片
        img_array = cv2.imread(os.path.join(path,img) ,cv2.IMREAD_GRAYSCALE)  # 转化成array
        plt.imshow(img_array, cmap='gray')  # 转换成图像展示
        plt.show()  # display!

        break  # 我们作为演示只展示一张，所以直接break了
    break  #同上
    print('1')
    print(img_array)
