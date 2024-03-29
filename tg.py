import asyncio
import sys, os, time, datetime
import pymongo
from pymongo.errors import BulkWriteError
from multiprocessing import Process, Queue

from py_ext.lzma import decompress, compress_files
from py_ext.tool import init_logger, log
from py_ext.wechat import send_wx

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel

from FastTelethon import download_file, upload_file

from helper import load_session_string, save_session_string, progress_cb
from helper import update_done_file, is_done_file
from compress import compress_date

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

def insert_data(datas, col):
    # 插入数据库
    try:
        col.insert_many(datas, ordered=False)
    except BulkWriteError:
        pass
    except Exception as e:
        raise

def handle_file(file_path):
    # 判断文件是否存在
    if not os.path.exists(file_path):
        return True

    # 解压文件
    try:
        decompress(file_path)
    except:
        return False

    file = os.path.basename(file_path)

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

    try:
        # 解析数据
        datas = []
        for data in parser:
            datas.append(data)

            if len(datas) == 30:
                # 插入数据库
                insert_data(datas, col)

                datas = []

        if datas:
            # 插入数据库
            insert_data(datas, col)

        return True
    except Exception as e:
        log(f"Error: {e}")
        return False

async def sender():
    await get_channel()
    
    while True:

        # 获取文件列表
        files = os.listdir(path)
        
        for file in files:

            _file = os.path.join(path, file)

            # 获取文件修改时间
            mtime = os.path.getmtime(_file)

            # 获取文件创建时间
            ctime = os.path.getctime(_file)

            cur_t = time.time()
            if not is_done_file(file) and mtime < cur_t - 60 and ctime < cur_t - 60: 
                # 如果有新文件，修改时间为1min前，上传到频道
                log(f"Uploading{file}")
                # await client.send_file(entity, _file, progress_callback=progress_cb)

                with open(_file, "rb") as out:
                    media = await upload_file(client, out, file, progress_callback=progress_cb)
                    await client.send_file(entity, media)

                update_done_file(file)

                log("删除原文件")
                os.remove(_file)
                
        await asyncio.sleep(30)

def updater(update_q):
    while not update_q.empty():
        _name = update_q.get()
        update_done_file(_name)
        log(f"File Updated: {_name}")

def saver(job_q, update_q, id):
    log(f"[{id}]Saver Started")
    while True:
        _file = job_q.get()
        log(f"[{id}]File Received: {_file}")

        # 处理数据
        if not handle_file(_file):
            msg = f"[{id}]File Handled Error: {_file}"
            send_wx(msg)
            log(msg)
            continue

        # 分割文件名
        _name = os.path.basename(_file)
        update_q.put(_name)
        log(f"[{id}]File Handled, send to updater {_name}")


async def receiver():
    await get_channel()

    jobs = 3

    job_q = Queue(maxsize=jobs)
    update_q = Queue()

    # 启动saver进程 * 3
    p_list = []
    for i in range(jobs):
        p_list.append(Process(target=saver, args=(job_q, update_q, i)))
        p_list[-1].start()

    working_list = []

    while True:
        messages = client.iter_messages(entity, reverse=True)

        # 循环遍历消息并筛选出包含文件的消息
        async for message in messages:

            # 检查是否处理更新
            updater(update_q)

            try:
                if not (message.file and message.file.name and not is_done_file(message.file.name) and message.file.name not in working_list):
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

            except Exception as e:
                log(f"Error: {e}")
                break

            # 不在此处压缩打包，影响效率
            # log("check compress_date")
            # # 压缩打包原始raw文件
            # time_ms = message.file.name.split("_")[-1][:-3]
            # file_date = datetime.datetime.fromtimestamp(int(time_ms)).date()
            # if file_date > date:
            #     compress_date(date, path)
            #     date = file_date
            #     log(f"打包完成, 更新日期 > {date}")

        await asyncio.sleep(60 * 5)

if __name__ == "__main__":

    # handle_file(r"\\192.168.100.203\wd_media\BINANCE_DATA\trade_1710289475620")

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

    # 初始化数据库
    if role == "receiver":
        client = pymongo.MongoClient()
        db = client['binance']
        col_trade = db['trade']
        col_depth = db['depth']
        col_trade.create_index([
                ('symbol', 1),
                ('save_timestamp', 1),
                ("id", 1)
            ], unique=True)
        col_depth.create_index([
                ('symbol', 1),
                ('save_timestamp', 1),
                ("id", 1)
            ], unique=True)

    # 创建客户端
    client = TelegramClient(StringSession(load_session_string()), api_id, api_hash)
    func = sender if role == "sender" else receiver
    with client:
        client.loop.run_until_complete(func())
