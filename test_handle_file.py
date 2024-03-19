import pymongo, os

from tg import insert_data
from binance_paser import trade, depth

# 数据库
client = None
db = None   
col_trade = None
col_depth = None

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

if __name__ == "__main__":
    client = pymongo.MongoClient("192.168.100.203")
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


    handle_file(r"Z:\BINANCE_DATA\depth_10_1710770400000")

    # 处理数据
    # folder = r'\\192.168.100.203\wd_media\BINANCE_DATA'
    # for file in os.listdir(folder):
    #     handle_file(os.path.join(folder, file))