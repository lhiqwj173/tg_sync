import sys, os, time
import asyncio

import pymongo

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel

from FastTelethon import download_file, upload_file

from helper import load_session_string, save_session_string, progress_cb
from helper import update_done_file, is_done_file

from binance_paser import trade, depth

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

def handle_file(file):
    parser = None
    col = None
    if 'trade' in file:
        parser = trade(file)
        col = col_trade
    elif 'depth_10_100' in file:
        parser = depth(file)
        col = col_depth
    else:
        return

    # 解析数据
    datas = [data for data in parser]

    # 插入数据库
    try:
        col.insert_many(datas, ordered=False)
    except Exception as e:
        print(f'error: {file}')
        raise e

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
                print("Uploading", file)
                # await client.send_file(entity, _file, progress_callback=progress_cb)

                with open(_file, "rb") as out:
                    media = await upload_file(client, out, file, progress_callback=progress_cb)
                    await client.send_file(entity, media)

                update_done_file(file)

                print("删除原文件")
                os.remove(_file)
        await asyncio.sleep(30)

async def receiver():
    await get_channel()
    
    while True:

        messages = client.iter_messages(entity)

        # 循环遍历消息并筛选出包含文件的消息
        async for message in messages:
            if message.file and message.file.name:
                if is_done_file(message.file.name):
                    continue

                print("-----------")
                print("File Name:", message.file.name)
                print("File Size:", message.file.size)

                # 使用 download_file() 方法下载文件
                # await client.download_media(message, file=os.path.join(path, message.file.name), progress_callback=progress_cb)
                _file = os.path.join(path, message.file.name)
                with open(_file, "wb") as out:
                    await download_file(client, message.file, out, progress_callback=progress_cb)
                print("File Downloaded")
                update_done_file(message.file.name)

                # 处理数据
                handle_file(_file)

                print("-----------")
        await asyncio.sleep(30)

if __name__ == "__main__":

    # 获取命令行参数
    if len(sys.argv) != 6:
        print("Usage: python tg.py <sender/receiver> <api_id> <api_hash> <name> <path>")
        print("Example: python tg.py sender 123456 abcdefghij1234567890 channel_name /path/to/folder")
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
                ('save_timestamp', 1)
            ], unique=True)
        col_depth.create_index([
                ('symbol', 1),
                ('save_timestamp', 1)
            ], unique=True)

    # 创建客户端
    client = TelegramClient(StringSession(load_session_string()), api_id, api_hash)
    func = sender if role == "sender" else receiver
    with client:
        client.loop.run_until_complete(func())
