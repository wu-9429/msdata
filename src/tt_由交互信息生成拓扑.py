#! /usr/bin/python3

import os
import time
import math
import random
import networkx as nx


# 获取指定目录下，所有 txt 文件的文件名
def file_name_list_alpha(file_dir):
    files = []      # list 存储路径下所有文件名称
    for file in os.listdir(file_dir):
        if os.path.splitext(file)[1] == '.txt':
            files.append(file_dir + file)
    return files


# # 为了加快拓扑产生，这里对数据处理结果文件进行筛选，选择那些生成拓扑时才用到的数据
def car_log_process_for_topu(file_dir, var_start_time, dur_time):
    files = file_name_list_alpha(file_dir)      # 读取 file_dir 文件夹下所有文件名
    files.sort()
    for file in files:
        car_log_list = []   # 存储节点的交互信息，节点之间的交互次数与交互持续时间
        with open(file, 'r') as f:
            # print('now process: ', file)
            for line in f:
                if len(line.split(',')) != 5:
                    continue
                vnuma, vnumb, curr_time, _, _ = [j for j in line.split(',')]
                if vnuma == vnumb:
                    continue
                curr_time = time.mktime(time.strptime(
                    curr_time, "%Y-%m-%d %H:%M:%S"))
                if curr_time < var_start_time or curr_time > var_start_time + dur_time:
                    continue
                car_log_list.append(line)

            # 将节点的交互次数与交互持续时间写入文件
            with open(f'./carlog/logpro/{file[13:]}', 'w') as f:
                for i in car_log_list:
                    f.write(i)
    return './carlog/logpro/'


#  移除与其他节点的交互记录少于 con_num 次的节点
def remove_some_node(file_dir, con_num):
    files = file_name_list_alpha(file_dir)
    files.sort()
    tem_node_map = {}   # 记录节点与其他所有节点的交互次数
    node_list = []      # list 存储保留的节点
    for file in files:
        print('now remove: ', file)
        with open(file, 'r') as f:
            for line in f:
                if len(line.split(',')) != 5:
                    continue
                vnuma, vnumb, _, _, _ = [j for j in line.split(',')]
                if vnuma in tem_node_map:
                    tem_node_map[vnuma] += 1
                else:
                    tem_node_map[vnuma] = 1
                if vnumb in tem_node_map:
                    tem_node_map[vnumb] += 1
                else:
                    tem_node_map[vnumb] = 1
    for i in tem_node_map:
        if tem_node_map[i] >= con_num:
            node_list.append(i)
    tem_node_map.clear()
    return files, node_list


#  使用 car_con_log_pro （存储节点之间的交互次数与交互持续时间）文件产生拓扑
# 每次产生 5 张拓扑，存入 graphs 中
def generate_topology(files, node_list, var_start_time, topu_dist, var_through_time=30, topu_num=5):
    graphs = {}     # 存储拓扑图
    for i in range(topu_num):
        graphs[i] = nx.Graph()
    for file in files:
        # print('now gen: ', file)
        with open(file, 'r') as f:
            for line in f:
                if len(line.split(',')) != 5:
                    continue
                vnuma, vnumb, start_time, curr_dist, _ = [j for j in line.split(',')]
                if float(curr_dist) > topu_dist:
                    continue
                start_time = time.mktime(time.strptime(start_time, "%Y-%m-%d %H:%M:%S"))
                if (vnuma not in node_list) or (vnumb not in node_list) or start_time < var_start_time:
                    continue
                i = int((start_time - var_start_time) / var_through_time)
                if i >= topu_num or i < 0:
                    continue
                # graphs[i].add_edge(vnuma, vnumb)
                # 插入下标，防止溢出
                graphs[i].add_edge(node_list.index(vnuma), node_list.index(vnumb))
    return graphs


#  输出上一步产生的拓扑到文件
def output_topology(graphs, car_num, tit):
    for i in graphs:
        with open(f'{tit}{i}.txt', 'w') as f:
            f.write(f'{car_num}\n')
            for j in range(car_num):
                for k in range(car_num):
                    if j in graphs[i] and k in graphs[i]:
                        if k in graphs[i].neighbors(j):
                            f.write(f'{j} {k}\n')


# main function
if __name__ == "__main__":
    var_start_time = 1202094000 + 150     # 2008:02:04 11:00:00 + 150s (+9)

    # # # 选取生成拓扑用的时间段的数据
    # 第一次运行时打开，之后注释此行
    file_dir = car_log_process_for_topu('./carlog/log/', var_start_time, 1200)

    # # 从所有处理后的记录中移除与其他节点的交互记录少于 con_num 次的节点
    # files 为所有文件名，node_list 为筛选后的节点列表
    con_num = 2
    files, node_list = remove_some_node('./carlog/logpro/', con_num)

    print('node_list len: ', len(node_list))

    # 生成拓扑时的节点数
    tem_car_num = [9000, 7200, 5400, 3600, 1800]    # 生成拓扑的节点数列表
    # for i in [9000, 7200, 5400, 3600, 1800]:
    #     if len(node_list) >= i:
    #         tem_car_num.append(i)
    #         break
    tem_time_for_eve = [30, 45, 60, 90, 120]
    topu_dist = 1000     # 生成拓扑时边的生成条件，小于 800 米时才生成边
    for tim_for_eve in tem_time_for_eve:
        for car_num in tem_car_num:
            topu_group = 1      # 生成拓扑的组数
            topu_num = 6        # 每组生成拓扑数
            random.shuffle(node_list)
            nl = node_list[:car_num]    # 随机选择 car_num 个节点
            print('node_list: ', len(nl))
            graphs = generate_topology(
                files, nl, var_start_time, topu_dist, tim_for_eve, topu_num*topu_group)
            output_topology(
                graphs, car_num, f'topu/topu_08/topu_gen_origin/topu_06/{car_num}_{tim_for_eve}_')



# # 由MS数据集生成的交互信息来生成拓扑文件
# # 1.使用 car_log_process_for_topu 来截取部分时间的车辆交互信息
