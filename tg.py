import asyncio
from collections import OrderedDict
import sys, os, time, datetime
import pymongo, subprocess
from pymongo.errors import BulkWriteError
from multiprocessing import Process, Queue

from py_ext.lzma import decompress, compress_files
from py_ext.tool import init_logger, log
from py_ext.wechat import send_wx

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel
from telethon import errors
from telethon.errors.rpcerrorlist import FileReferenceExpiredError 

from FastTelethon import download_file, upload_file

from helper import load_session_string, save_session_string, progress_cb
from helper import update_done_file, is_done_file
from compress import compress_date

from py_ext.tg import tg_upload
from py_ext.alist import alist
from binance_paser import trade, depth
"""
提速
https://gist.github.com/painor/7e74de80ae0c819d3e9abcf9989a8dd6
"""
init_logger("sync_tg_file")

entity = None
async def get_channel():
    global entity

    # 储存session
    save_session_string(client.session.save())

    async for dialog in client.iter_dialogs():
        if dialog.name == name:
            # 频道文件列表
            channel_username = dialog.id  # 替换为频道的用户名或 ID
            entity = await client.get_entity(channel_username)  # 获取频道实体对象
            break

# 数据库
client = None
db = None   
col_trade = None
col_depth = None

def check_need_write(datas, _wait_write):
    if "bid1_price" in datas[0]:
        for data in datas:
            if data['symbol'] in ['ethfdusd', 'ethusdt', 'btcfdusd', 'btcusdt']:
                dt = datetime.datetime.fromtimestamp(int(data['save_timestamp'] / 1000))
                date = dt.date()
                if date not in _wait_write:
                    _wait_write[date] = []
                _wait_write[date].append(data)

def run_kaggle_notebook(notebook):
    # 切换到指定目录
    _path = os.getcwd()

    _kaggle_folder = os.path.join('/tmp', 'kaggle')
    if not os.path.exists(_kaggle_folder):
        os.mkdir(_kaggle_folder)
    os.chdir(_kaggle_folder)

    # 定义要执行的命令
    command2 = f"kaggle kernels pull {notebook} -m"
    command3 = "kaggle kernels push"

    # 使用subprocess运行命令
    subprocess.run(command2, shell=True)
    subprocess.run(command3, shell=True)

    # 恢复
    os.chdir(_path)

def compress_date_file_to_tg(new_date):
    new_date = str(new_date)

    # 按日期整理
    folder_datas = {}
    for file in os.listdir(daily_folder):
        if 'depth' not in file or '.csv' not in file:
            continue

        date = file.split('_')[0]
        if len(date) != 10 or date >= new_date:
            continue

        if date not in folder_datas:
            folder_datas[date] = []
        folder_datas[date].append(file)

    # ALIST
    a = alist('admin', 'LHss6632673')
    cur_files = [i['name'] for i in a.listdir('/')]
    if 'daily_bin_data' not in cur_files:
        log(f'mkdir: /daily_bin_data')
        a.mkdir('/daily_bin_data')

    # 删除一个月以前的文件
    need_del_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
    for file_dict in a.listdir('/daily_bin_data'):
        file = file_dict['name']
        if '.7z' not in file:
            continue
        date = file.replace('.7z', '')
        if date < need_del_date:
            log(f"delete {date}")
            a.delete(f'/daily_bin_data/{date}.7z')

    for date in folder_datas:
        log(f"compress {date} -> {date}.7z")
        out_file = os.path.join(daily_folder, f'{date}.7z')
        compress_files([os.path.join(daily_folder,i) for i in folder_datas[date]], out_file, 9)

        # # 上传到tg 
        # log(f"tg upload {out_file}")
        # ses = '1BVtsOGYBu8XQemcEajKaoxqYAUOVIlF-Dyb9zCzR5Na9DJKVTC03W23hU6wB2wkyrMfkCqEXasFyPBbEd5p3TLoGktw5quatBHmj5ln7cm8lW5kmeW4RaK-idCzswxPEgX_oiz6NqNlG5I5HMifzMcORrmTtstshq93AaidszKe3LCTjQ09qpt3ORi66RipkdI-Q5qmaFfkDMKIiEtQWMa1MXzZ6d8-rt4OFrx8M545Z7budJGyVxvzxskH0uq9gNC4lPP-p97irGafb9Vn26ZvrU_ETMeadoh5qKqs_IT2_AFgZeAa53PnYH_qbcaO2AMRWmsHxMlocv4baVk_PHJfIMooPiDU='
        # tg_upload(ses, out_file, 'bin_daily')
        a.upload(out_file, '/daily_bin_data')

        # # # 删除源文件
        # log(f"delete {out_file}")
        # os.remove(out_file)

    if len(folder_datas) > 0:
        # 重启kaggle计算
        notebooks = ['qtz173/bin-daily-run', 'qtz173/bin-train-data']
        for notebook in notebooks:
            log(f"启动kaggle {notebook}")
            run_kaggle_notebook(notebook)

def write_daily(_wait_write, id):
    _wait_write = dict(sorted(_wait_write.items()))

    for date in _wait_write:
        log(f"write_daily {date}")

        # 检查是否是新的一天数据
        # 文件夹中没有 f'{date}_depth' 开头的文件
        new_day = True
        date_head = f'{date}_depth'
        for file in os.listdir(daily_folder):
            if file.startswith(date_head):
                # 已经又该日期文件存在，不是新的一天
                new_day = False
                break
        if new_day:
            # 打包压缩所有的日期文件
            # 发送到tg频道
            compress_date_file_to_tg(date)

        # # 启动kaggle计算
        # notebook = 'qtz173/bin-daily-run'
        # log(f"启动kaggle {notebook}")
        # run_kaggle_notebook(notebook)

        file = os.path.join(daily_folder, f'{date}_depth_{id}.csv')
        # 如果文件不存在，需要写入列名
        if os.path.exists(file) == False:
            with open(file, 'w') as f:
                f.write('datetime,code,卖1价,卖1量,买1价,买1量,卖2价,卖2量,买2价,买2量,卖3价,卖3量,买3价,买3量,卖4价,卖4量,买4价,买4量,卖5价,卖5量,买5价,买5量,卖6价,卖6量,买6价,买6量,卖7价,卖7量,买7价,买7量,卖8价,卖8量,买8价,买8量,卖9价,卖9量,买9价,买9量,卖10价,卖10量,买10价,买10量\n')

        with open(file, 'a', buffering=8192 * 2) as f:
            for data in _wait_write[date]:
                f.write(
                    # 'datetime', 'code', '卖1价', '卖1量', '买1价', '买1量', '卖2价', '卖2量', '买2价', '买2量', '卖3价', '卖3量', '买3价', '买3量', '卖4价', '卖4量', '买4价', '买4量', '卖5价', '卖5量', '买5价', '买5量', '卖6价', '卖6量', '买6价', '买6量', '卖7价', '卖7量', '买7价', '买7量', '卖8价', '卖8量', '买8价', '买8量', '卖9价', '卖9量', '买9价', '买9量', '卖10价', '卖10量', '买10价', '买10量'
                    f'{data["save_timestamp"]},{data["symbol"]},'
                )

                for i in range(10):
                    f.write(str(data[f'ask{i+1}_price']) + ',')
                    f.write(str(data[f'ask{i+1}_vol']) + ',')
                    f.write(str(data[f'bid{i+1}_price']) + ',')
                    f.write(str(data[f'bid{i+1}_vol']))

                    if i < 9:
                        f.write(',')
                    
                f.write('\n')

def insert_data(datas, col):
    # 插入数据库
    try:
        col.insert_many(datas, ordered=False)
    except BulkWriteError:
        pass
    except Exception as e:
        raise

def handle_file(file_path, id, need_decompress=True):
    file = os.path.basename(file_path)

    # 判断文件是否存在
    if not os.path.exists(file_path):
        log(f"{file} 文件不存在")
        return True

    # 解压文件
    if need_decompress:
        try:
            log(f"{file} 解压文件")
            decompress(file_path, 'decompress_temp')

            # 移出文件
            folder, file_name = os.path.split(file_path)
            temp_file = os.path.join(folder, 'decompress_temp', 'data', file_name)
            os.rename(temp_file, file_path)
        
        except Exception as e:
            log(f"{file} 解压文件失败\n{e}")
            raise
            return False

    log(f"{file} 分配 parser")
    parser = None
    col = None
    if 'trade' in file:
        parser = trade(file_path)
        col = col_trade
    elif 'depth' in file:
        parser = depth(file_path)
        col = col_depth
    else:
        return False

    t0 = time.time()
    log(f"{file} 解析数据")
    datas = []
    wait_write = {}
    for data in parser:
        datas.append(data)

        if len(datas) == 30:
            check_need_write(datas, wait_write)

            # 不储存 mongo
            # 插入数据库
            # insert_data(datas, col)

            datas = []

    if datas:
        check_need_write(datas, wait_write)

        # 不储存 mongo
        # 插入数据库
        # insert_data(datas, col)

    log(f'{file} 解析数据完毕, 耗时:{time.time() - t0:.2f}s')
    t0 = time.time()

    if wait_write:
        # 写入每日文件
        log(f"{file} 记录文件")
        write_daily(wait_write, id)
        log(f'{file} 写入文件完毕, 耗时:{time.time() - t0:.2f}s')

    return True

async def sender():
    await get_channel()
    
    while True:

        # 获取文件列表
        files = os.listdir(path)
        msgs = []
        for i in files:
            t = int(split('_')[-1])
            msgs.append(t_msg(t, i))
        msgs = sorted(msgs, key=lambda x: x.timestamp)
        
        for obj in msgs:
            file = obj.tg_msg
            _file = os.path.join(path, file)

            # 获取文件修改时间
            mtime = os.path.getmtime(_file)

            # 获取文件创建时间
            ctime = os.path.getctime(_file)

            cur_t = time.time()
            if mtime < cur_t - 60 and ctime < cur_t - 60: 
                # 如果有新文件，修改时间为1min前，上传到频道
                log(f"Uploading {file}")

                succsee = False
                for i in range(5):
                    try:
                        with open(_file, "rb") as out:
                            media = await upload_file(client, out, file, progress_callback=progress_cb)
                            await client.send_file(entity, media)
                        succsee = True
                        break
                    except errors.FloodWaitError as e:
                        log('Have to sleep', e.seconds, 'seconds, then will retry')
                        time.sleep(e.seconds)
                    except Exception as e:
                        raise e
                if not succsee:
                    raise Exception(f"[{file}]Upload Failed")

                log("删除原文件")
                os.remove(_file)

        await asyncio.sleep(30)

def updater(update_q, working_list, done_list):
    while not update_q.empty():
        _name = update_q.get()
        update_done_file(_name)
        working_list.remove(_name)
        done_list.append(_name)
        log(f"File Updated: {_name}")

def saver(job_q, update_q, id):
    log(f"[{id}]Saver Started")
    while True:
        _file = job_q.get()
        log(f"[{id}]File Received: {_file}")

        # 处理数据
        if not handle_file(_file, id):
            msg = f"[{id}]File Handled Error: {_file}"
            send_wx(msg)
            log(msg)
            continue

        # 分割文件名
        _name = os.path.basename(_file)
        update_q.put(_name)
        log(f"[{id}]File Handled, send to updater {_name}")

class t_msg():
    def __init__(self, timestamp, tg_msg):
        self.timestamp = timestamp
        self.tg_msg = tg_msg

async def receiver():
    await get_channel()

    jobs = 1

    job_q = Queue(maxsize=jobs)
    update_q = Queue()

    # 启动saver进程 * 3
    p_list = []
    for i in range(jobs):
        p_list.append(Process(target=saver, args=(job_q, update_q, i)))
        p_list[-1].start()

    working_list = []
    done_list = []
    while True:
        messages = client.iter_messages(entity, reverse=True)

        msgs = []
        async for message in messages:
            if message.file and message.file.name:
                t = int(message.file.name.split('_')[-1])
                msgs.append(t_msg(t, message))
        msgs = sorted(msgs, key=lambda x: x.timestamp)

        # with open('messages.txt', 'w') as f:
        #     for msg in msgs:
        #         f.write(f'[{msg.timestamp}] {msg.tg_msg.file.name}\n')
        
        # sys.exit(0)

        # 循环遍历消息并筛选出包含文件的消息
        success = 0
        for msg in msgs:

            # 检查是否处理更新
            # log("check updater")
            updater(update_q, working_list, done_list)
            # log(f"working_list: {working_list}")
            # log(f"done_list: {done_list}")

            try:
                message = msg.tg_msg
                if is_done_file(message.file.name) or message.file.name in working_list or message.file.name in done_list:
                    continue

                log("-----------")
                log(f"File Name: {message.file.name}")
                log(f"File Size: {message.file.size}")

                # 使用 download_file() 方法下载文件
                # await client.download_media(message, file=os.path.join(path, message.file.name), progress_callback=progress_cb)
                _file = os.path.join(path, message.file.name)
                with open(_file, "wb") as out:
                    await download_file(client, message.document, out, progress_callback=progress_cb)

                job_q.put(_file)
                log(f"File Downloaded, send to saver {message.file.name}")
                working_list.append(message.file.name)
                log("-----------")

                success = 1

            except FileReferenceExpiredError:
                # 重新获取messages遍历
                log('FileReferenceExpiredError retry')
                break

            except Exception as e:
                raise Exception(f'{e}')

            # 不在此处压缩打包，影响效率
            # log("check compress_date")
            # # 压缩打包原始raw文件
            # time_ms = message.file.name.split("_")[-1][:-3]
            # file_date = datetime.datetime.fromtimestamp(int(time_ms)).date()
            # if file_date > date:
            #     compress_date(date, path)
            #     date = file_date
            #     log(f"打包完成, 更新日期 > {date}")

        # 成功下载文件后不需要等待，下载过程耗费了时间
        if not success:
            await asyncio.sleep(60 * 5)

if __name__ == "__main__":

    # handle_file(r"\\192.168.100.203\wd_media\BINANCE_DATA\depth_10_1720426109361", 0, False)

    # 获取命令行参数
    if len(sys.argv) != 6:
        log("Usage: python tg.py <sender/receiver> <api_id> <api_hash> <name> <path>")
        log("Example: python tg.py sender 123456 abcdefghij1234567890 channel_name /path/to/folder")
        sys.exit(1)

    role = sys.argv[1]
    api_id = int(sys.argv[2])
    api_hash = sys.argv[3]
    name = sys.argv[4]
    path = sys.argv[5]
    daily_folder = ''

    # 初始化数据库
    if role == "receiver":
        daily_folder = os.path.join(os.path.dirname(path), 'binance_daily_mean_std_data')
        os.makedirs(daily_folder, exist_ok=True)

        # 不储存到mongo
        # client = pymongo.MongoClient()
        # db = client['binance']
        # col_trade = db['trade']
        # col_depth = db['depth']
        # col_trade.create_index([
        #         ('symbol', 1),
        #         ('save_timestamp', 1),
        #         ("id", 1)
        #     ], unique=True)
        # col_depth.create_index([
        #         ('symbol', 1),
        #         ('save_timestamp', 1),
        #         ("id", 1)
        #     ], unique=True)

    # 创建客户端
    client = TelegramClient(StringSession(load_session_string()), api_id, api_hash)
    func = sender if role == "sender" else receiver
    with client:
        client.loop.run_until_complete(func())
