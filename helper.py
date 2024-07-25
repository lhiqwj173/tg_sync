import os,time

def save_session_string(session_string):
    with open('session.txt', 'w') as f:
        f.write(session_string)

def load_session_string():
    if not os.path.exists('session.txt'):
        return None

    with open('session.txt', 'r') as f:
        return f.read()


# 已完成的文件列表
latest_time = 0
if os.path.exists('done_timestamp.txt'):
    # 读取已完成的文件列表
    with open('done_timestamp.txt', 'r') as f:
        latest_time = int(f.read().strip())

done_list = []
if os.path.exists('done.txt'):
    with open('done.txt', 'r') as f:
        done_list = f.read().splitlines()

def update_done_file(file_name):
    global latest_time, done_list

    # 向前推1000秒，避免 先处理的文件时间戳略大, 导致略小时间戳的文件无法被处理
    latest_time = int(file_name.split('_')[-1]) - 1000
    with open('done_timestamp.txt', 'w') as f:
        f.write(str(latest_time))

    done_list.append(file_name)
    with open('done.txt', 'a') as f:
        f.write(file_name + '\n')

def is_done_file(file_name):
    # 获取最新的时间
    # 最新时间前12小时的数据不再处理
    timestamp = int(file_name.split('_')[-1])
    return timestamp < latest_time or file_name in done_list

t = 0
def progress_cb(current, total):
    global t
    if t == 0:
        t = time.time()
        return

    cur_mb = current / 1024 / 1024
    cur_cost = time.time() - t

    speed = cur_mb / cur_cost
    pct = current / total
    remain = (total/ 1024 / 1024 - cur_mb) / speed
    print(f'done: {pct:.2%}, speed: {speed:.2f} MB/s remain: {remain:.2f}s', end='\r')