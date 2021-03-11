#! /usr/bin/python3

import os
import time
import math
import random
import networkx as nx
from multiprocessing import Process
from geopy.distance import geodesic
from scipy.interpolate import interp1d


# 从 node_print 文件中读取已处理过的节点的编号，存入 node_print 列表
def old_node_print(file):
    node_print = [ ]    # list 存储已处理的节点
    with open(file, 'r') as f:
        for line in f:
            if len(line.split()):
                node_print.append(line.split()[0])
    return node_print


# 读取某一车辆位置记录中时间在 log_start_time - log_end_time 内的内容，输出车辆编号，位置列表，时间列表
def open_text_with_start(file, log_start_time, log_end_time):
    vnum = 0
    vloc = []   # list 存储位置信息 (jd, wd)
    vtime = []  # list 存储时间信息
    ff = 0
    with open(file, 'r') as f:
        for line in f:
            if len(line.split(',')) != 4:
                continue
            num, tim, jd, wd = [j for j in line.split(',')]
            if float(jd) > 180 or float(jd) < -180:
                continue
            if float(wd) > 90 or float(wd) < -90:
                continue
            # 时间处理，字符串时间 --> 结构体时间 --> Unix 时间戳
            tim = time.mktime(time.strptime(tim, "%Y-%m-%d %H:%M:%S"))
            # 记录规定时间内的内容
            if tim > log_start_time and tim < log_end_time:
                if ff and tim != old_tim :
                    vnum = num
                    vtime.append(old_tim)
                    vloc.append((float(old_wd), float(old_jd)))
            # 记录第一个超出时间的内容
            elif tim > log_end_time:
                if vtime == [] :
                    break
                vtime.append(old_tim)
                vtime.append(tim)
                vloc.append((float(old_wd), float(old_jd)))
                vloc.append((float(wd), float(jd)))
                break
            old_tim, old_jd, old_wd = tim, jd, wd
            ff = 1
    return vnum, vloc, vtime


# 线性插值法，计算节点某一时刻的经度与纬度
def time_get_loc(tim, wd, jd, itim):
    res_wd = interp1d([tim[1], tim[0]], [wd[1], wd[0]], kind='linear')(itim)
    res_jd = interp1d([tim[1], tim[0]], [jd[1], jd[0]], kind='linear')(itim)
    return res_wd, res_jd


# 获取两个节点早期最近有记录的时间，从该时间开始计算两个节点是否交互
# 即当某时刻只有一个节点有时间记录时，跳过
def first_time(tima, timb):
    i = 0
    j = 0
    if i < len(tima) and j < len(timb) and tima[i] < timb[j]:
        while(i < len(tima)-1 and tima[i+1] < timb[j]):
            i += 1
    elif i < len(tima) and j < len(timb) and tima[i] > timb[j]:
        while(j < len(timb)-1 and tima[i] > timb[j+1]):
            j += 1
    i += 1
    j += 1
    return i, j


# 1. 获取指定目录下，编号小于 car_num 的 txt 文件的文件名（车辆文件名）
def file_name_list(file_dir, car_num):
    files = []      # list 存储路径下所有文件名称
    for file in os.listdir(file_dir):
        if os.path.splitext(file)[1] == '.txt':
            if int(os.path.splitext(file)[0]) <= car_num:  # select car_num cars(or nodes)
                files.append(file_dir + file)
    return files


# 获取指定目录下，所有 txt 文件的文件名
def file_name_list_alpha(file_dir):
    files = []      # list 存储路径下所有文件名称
    for file in os.listdir(file_dir):
        if os.path.splitext(file)[1] == '.txt':
            files.append(file_dir + file)
    return files


# # 2. remove some file
# # 1) long distance no GPS
# # 2) long time no GPS
# # 3) high speed (>= 120kmph)
# def remove_invalid_file(vnum_list, vloc_dict, vtime_dict):
#     tem_list = []
#     for vnum in vnum_list:
#         vloc = vloc_dict[vnum]
#         vtime = vtime_dict[vnum]
#         error_row = 0
#         cnt = 1
#         while(cnt < len(vloc)):
#             temp_time = vtime[cnt] - vtime[cnt-1]
#             temp_distance = geodesic(vloc[cnt-1], vloc[cnt]).meters
#             temp_speed = temp_distance / temp_time
#             if (temp_time > 180 and temp_distance > 400) or temp_speed > 28 :
#                 error_row += 1
#             if error_row > 4:
#                 break
#             cnt += 1
#         if error_row > 4:
#             continue
#         tem_list.append(vnum)
#     return tem_list


# 2.5 读取文件夹中所有文件内容到 dict
# 车辆编号为 dict 的 key，车辆的位置记录与时间记录为 dict 的 value
def files_to_dict(files, log_start_time, log_end_time):
    vnum_list = []      # list 存储所有节点编号
    vloc_dict = { }     # dict 存储对应节点编号的位置信息
    vtime_dict = { }    # dict 存储对应节点编号的时间信息
    for file in files:
        tem_num, tem_loc, tem_time = open_text_with_start(file, log_start_time, log_end_time)
        if tem_num:
            vnum_list.append(tem_num)
            vloc_dict[tem_num] = tem_loc
            vtime_dict[tem_num] = tem_time
    return vnum_list, vloc_dict, vtime_dict


# 3. 计算两个节点的交互信息，某一时刻两个节点的距离等等
# 按时间序列计算两个节点在该时刻的距离，当两个节点的距离小于 800 米时，存入 list
def car_contact_time_list(vnuma, vnumb, vloca, vlocb, vtimea, vtimeb):
    contact_log = []    # 存储节点的交互信息，某一时刻两个节点的距离等
    i, j = first_time(vtimea, vtimeb)
    time_pos = max(vtimea[i-1], vtimeb[j-1])    # 记录 time_pos 时刻两个节点的距离
    time_pos_t = min(vtimea[i-1], vtimeb[j-1])  # 记录前一个 time_pos 时刻，使两个 time_pos 跨度大于 5 秒，避免步进太小
    while(i < len(vtimea) and j < len(vtimeb)):
        time_pos = min(time_pos, vtimea[i], vtimeb[j])
        if i < len(vtimea) and time_pos == vtimea[i]:
            i += 1
        if j < len(vtimeb) and time_pos == vtimeb[j]:
            j += 1
        if i == len(vtimea) or j == len(vtimeb):
            break
        # 使两个 time_pos 跨度大于 5 秒，避免步进太小
        if time_pos != time_pos_t and time_pos - time_pos_t < 5:
            time_pos += 5
            continue
        time_pos_t = time_pos
        speeda = geodesic(vloca[i-1], vloca[i]).meters/(vtimea[i]-vtimea[i-1])
        speedb = geodesic(vlocb[j-1], vlocb[j]).meters/(vtimeb[j]-vtimeb[j-1])
        # 线性插值，得到两个节点某一时间的经度与纬度
        tem_loca = time_get_loc([vtimea[i], vtimea[i-1]], [vloca[i][0], vloca[i-1][0]],
                                [vloca[i][1], vloca[i-1][1]], time_pos)
        tem_locb = time_get_loc([vtimeb[j], vtimeb[j-1]], [vlocb[j][0], vlocb[j-1][0]],
                                [vlocb[j][1], vlocb[j-1][1]], time_pos)
        if speeda + speedb == 0:
            time_pos = min(vtimea[i], vtimeb[j])
            continue
        # 如果当前两个节点的距离小于 1000 米就记录下来，并计算两个节点可能分离的时间
        if geodesic(tem_loca, tem_locb).meters < 995:
            contact_log.append([vnuma, vnumb, time_pos, vloca[i],
                                vlocb[j], round(geodesic(tem_loca, tem_locb).meters, 3), round(speeda + speedb, 3)])
            add_time = (1000 - geodesic(tem_loca, tem_locb).meters) / \
                (speeda + speedb)
            time_pos += add_time + 5
        # 如果当前两个节点的距离大于 1000 米，就计算两个节点下次可能产生交互的时间
        elif geodesic(tem_loca, tem_locb).meters > 1005:
            add_time = (geodesic(tem_loca, tem_locb).meters - 1000) / \
                (speeda + speedb)
            time_pos += add_time + 5
        else:
            time_pos = min(vtimea[i], vtimeb[j])
    return contact_log


# 4. 输出所有节点间的交互信息，处理 1-1000 节点
# 进程 1，处理前 1000 个节点与其他节点的交互信息
def output_contact_log_01(vnum, vloc, vtime):
    # 如果程序中断，读取已处理过的节点的编号，node_print 文件
    if os.path.isfile('./carlog/node_print_01.txt'):
        node_print = old_node_print('./carlog/node_print_01.txt')
    else:
        node_print = [ ]        # list 存储已处理的节点
    cnt = len(node_print) + 1       # 交互信息输出计数
    ff = len(node_print) // 100 + 1     # 交互信息输出文件计数

    # 计算前 1000 个节点与其他节点的交互信息
    for i in range(1000):
        if vnum[i] not in node_print:
            print('now', vnum[i], '\t', cnt)
            start = time.time()
            if cnt % 100 == 0:
                ff += 1
            for j in range(len(vnum)):
                if (vnum[j] not in node_print) and (vnum[j] != vnum[i]):
                    # 计算两个节点的交互信息
                    contact_log = car_contact_time_list(vnum[i], vnum[j], vloc[vnum[i]], vloc[vnum[j]], vtime[vnum[i]], vtime[vnum[j]])
                    if contact_log == []:
                        continue
                    else:
                        # 将当前两个节点的交互信息写入文件
                        with open(f'./carlog/log/car_con_log{ff}.txt', 'a') as f:
                            for k in contact_log:
                                f.write(f'{k[0]},{k[1]},{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(k[2]))},{k[5]},{k[6]}\n')
                                # f.write(k[0] + ',' + k[1] + ',' +
                                #         time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(k[2])) + ',' + str(k[5]) + ',' + str(k[6]) + '\n')
            print('time cost: ', round(time.time() - start, 3))
            cnt += 1
            with open('./carlog/node_print_01.txt', 'a') as f:
                f.write(f'{vnum[i]}\n')
            node_print.append(vnum[i])
        # else :
        #     with open('./carlog/node_print_01.txt', 'a') as f:
        #         f.write(f'{vnum[i]}\told\n')


# 输出所有节点间的交互信息，处理 1000-2000 节点
# 进程 2，处理前 1000 - 2000 个节点与其他节点的交互信息
def output_contact_log_02(vnum, vloc, vtime):
    if os.path.isfile('./carlog/node_print_02.txt'):
        node_print = old_node_print('./carlog/node_print_02.txt')
    else:
        node_print = [ ]    # list 存储已处理的节点
    cnt = 1000 + len(node_print) + 1       # 交互信息输出计数
    ff = (1000 + len(node_print)) // 100 + 1     # 交互信息输出文件计数

    # 计算前 1000-2000 节点与其他节点的交互信息
    for i in range(1000, 2000):
        if vnum[i] not in node_print:
            print('now', vnum[i], '\t', cnt)
            start = time.time()
            if cnt % 100 == 0:
                ff += 1
            for j in range(1000, len(vnum)):
                # 计算两个节点的交互信息
                if (vnum[j] not in node_print) and (vnum[j] != vnum[i]):
                    contact_log = car_contact_time_list(vnum[i], vnum[j], vloc[vnum[i]], vloc[vnum[j]], vtime[vnum[i]], vtime[vnum[j]])
                    if contact_log == []:
                        continue
                    else:
                        # 将当前两个节点的交互信息写入文件
                        with open(f'./carlog/log/car_con_log{ff}.txt', 'a') as f:
                            for k in contact_log:
                                f.write(f'{k[0]},{k[1]},{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(k[2]))},{k[5]},{k[6]}\n')
                                # f.write(k[0] + ',' + k[1] + ',' +
                                #         time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(k[2])) + ',' + str(k[5]) + ',' + str(k[6]) + '\n')
            print('time cost: ', round(time.time() - start, 3))
            cnt += 1
            with open('./carlog/node_print_02.txt', 'a') as f:
                f.write(f'{vnum[i]}\n')
            node_print.append(vnum[i])
        # else :
        #     with open('./carlog/node_print_02.txt', 'a') as f:
        #         f.write(f'{vnum[i]}\told\n')


# # 5. 处理节点的交互信息，将节点关于时刻的距离信息转换为节点之间的交互次数与交互持续时间
# def car_log_process(file_dir):
#     files = file_name_list_alpha(file_dir)
#     for file in files:
#         car_log_list = []   # 存储节点的交互信息，节点之间的交互次数与交互持续时间
#         tem_vnuma = '0'
#         tem_vnumb = '0'
#         ff = 0
#         with open(file, 'r') as f:
#             print('now: ', file)
#             for line in f:
#                 if len(line.split(',')) != 5:
#                     continue
#                 vnuma, vnumb, curr_time, _, speed = [j for j in line.split(',')]
#                 if vnuma == vnumb :
#                     continue
#                 curr_time = time.mktime(time.strptime(
#                     curr_time, "%Y-%m-%d %H:%M:%S"))
#                 # 若记录的两个时间跨度与速度的积大于 1000，则认为发生了第二次交互
#                 if vnuma != tem_vnuma or vnumb != tem_vnumb or float(curr_time - tem_end_time) * float(speed) > 1000.0 :  # 1000 meter no log
#                     if ff:
#                         car_log_list.append(
#                             [tem_vnuma, tem_vnumb, tem_start_time, tem_end_time - tem_start_time])
#                     tem_start_time = curr_time
#                 tem_vnuma, tem_vnumb = vnuma, vnumb
#                 tem_end_time = curr_time
#                 ff = 1
#         # 将节点的交互次数与交互持续时间写入文件
#         with open('./carlog/car_con_log_pro.txt', 'a') as f:
#             for i in car_log_list:
#                 f.write(f'{i[0]},{i[1]},{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(i[2]))},{i[3]}s\n')
#                 # f.write(i[0] + ',' + i[1] + ',' +
#                 #         time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(i[2])) + ',' + str(i[3]) + 's' + '\n')
#     return './carlog/car_con_log_pro.txt'


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
                if vnuma == vnumb :
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


# 6. 移除与其他节点的交互记录只有 1 次的节点
def remove_some_node(file_dir):
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
        if tem_node_map[i] >= 2:
            node_list.append(i)
    tem_node_map.clear()
    return files, node_list


# 7. 使用 car_con_log_pro （存储节点之间的交互次数与交互持续时间）文件产生拓扑
# 每次产生 6 张拓扑，存入 graphs 中
def generate_topology(files, node_list, var_start_time, topu_dist, var_through_time = 30, topu_num = 5):
    graphs = {}     # 存储拓扑图
    for i in range(topu_num):
        graphs[i] = nx.Graph()
    for file in files:
        # print('now gen: ', file)
        with open(file, 'r') as f:
            for line in f:
                if len(line.split(',')) != 5:
                    continue
                vnuma, vnumb, start_time, curr_dist, _ = [
                    j for j in line.split(',')]
                if float(curr_dist) > topu_dist:
                    continue
                start_time = time.mktime(time.strptime(
                    start_time, "%Y-%m-%d %H:%M:%S"))
                if (vnuma not in node_list) or (vnumb not in node_list) or start_time < var_start_time :
                    continue
                i = int((start_time - var_start_time) / var_through_time)
                if i >= topu_num or i < 0 :
                    continue
                # graphs[i].add_edge(vnuma, vnumb)
                # 插入下标，防止溢出
                graphs[i].add_edge(node_list.index(vnuma), node_list.index(vnumb))
    return graphs


# 8. 输出上一步产生的拓扑到文件
def output_topology(graphs, car_num, tit):
    for i in graphs:
        with open(f'{tit}{i}.txt', 'w') as f:
            f.write(f'{car_num}\n')
            # for j in graphs[i]:
            for j in range(car_num):
                for k in range(car_num):
                    if j in graphs[i] and k in graphs[i]:
                        if k in graphs[i].neighbors(j):
                            f.write(f'{j} {k}\n')


# main function
if __name__ == "__main__":
    car_num = 11357     # 要处理的车辆数
    log_start_time = 1202086800     # 2008:02:04 09:00:00
    log_end_time =  1202101200      # 2008:02:04 13:00:00
    var_start_time = 1202094000     # 2008:02:04 11:00:00

    # # 获取编号小于 car_num 的 txt 文件的文件名（车辆文件名）
    files = file_name_list('./data/', car_num)
    # # 读取文件夹中所有文件内容到 dict
    # # 车辆编号为 dict 的 key，车辆的位置记录与时间记录为 dict 的 value
    vnum_list, vloc_dict, vtime_dict = files_to_dict(files, log_start_time, log_end_time)    # returm num list, loc dict and time dict
    # # # 移除无效的文件，不进行该项
    # # vnum_list = remove_invalid_file(vnum_list, vloc_dict, vtime_dict)

    # # vnum_list 排序
    vnum_list.sort()

    ### 多进程运行
    # 创建两个进程
    por_01 = Process(target=output_contact_log_01, args=(vnum_list, vloc_dict, vtime_dict))
    por_02 = Process(target=output_contact_log_02, args=(vnum_list, vloc_dict, vtime_dict))
    por_01.start()
    por_02.start()
    por_01.join()
    por_02.join()
    ###

    # # 之前多余的操作
    # con_pro_file = car_log_process('./carlog/log/')

    # # # 选取生成拓扑用的时间段的数据
    # file_dir = car_log_process_for_topu('./carlog/log/', var_start_time, 1200)

    # # 从所有处理后的记录中移除与其他节点的交互记录只有 1 次的节点
    # files 为所有文件名，node_list 为筛选后的节点列表
    files, node_list = remove_some_node('./carlog/log/')
    
    # 生成拓扑时的节点数
    tem_car_num = [1800, 3600, 5400, 7200, 9000]    # 生成拓扑的节点数列表
    topu_dist = 800     # 生成拓扑时边的生成条件，小于 800 米时才生成边
    for car_num in tem_car_num:
        topu_group = 3      # 生成拓扑的组数
        topu_num = 5        # 每组生成拓扑数
        tim_for_eve = 30    # 生成拓扑的间隔时间
        nl = [ ]            # 随机选择 car_num 个节点
        for i in range(car_num):
            random.shuffle(node_list)
            nl = node_list[:car_num]
            # a = int(random.random() * len(node_list))
            # while node_list[a] in nl:
            #     a = int(random.random() * len(node_list))
            # nl.append(node_list[a])
        print('node_list: ', len(nl))
        # # 将 var_start_time 起后时间分段存储，以 tim_for_eve 为间隔，生成 topu_num*topu_group 组
        tem_graphs = generate_topology(files, nl, var_start_time, topu_dist, tim_for_eve, topu_num*topu_group)
        # # 生成 topu_group 组拓扑
        for i in range(topu_group):
            print('now gen: ', i)
            graphs = {}
            # # 每组拓扑生成 topu_num 个拓扑
            for j in range(topu_num):
                graphs[j] = tem_graphs[i*j + j]
                # # 合并 tem_graph
                for k in range(i):
                    kk = i*j + j + k + 1
                    for vv in tem_graphs[kk]:
                        for vvv in tem_graphs[kk]:
                            if vv in tem_graphs[kk].neighbors(vvv):
                                graphs[j].add_edge(vv, vvv)
            # # 输出 graphs 中的拓扑
            output_topology(graphs, car_num, f'./carlog/topu/{car_num}_{i*tim_for_eve + tim_for_eve}_')
    