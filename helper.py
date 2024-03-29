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
done_files = set()
if os.path.exists('done.txt'):
    # 读取已完成的文件列表
    with open('done.txt', 'r') as f:
        done_files = set(f.read().split('\n'))

def update_done_file(file):
    global done_files

    done_files.add(file)
    with open('done.txt', 'w') as f:
        for file in done_files:
            f.write(file + '\n')

def is_done_file(file):
    return file in done_files

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