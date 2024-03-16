import datetime,sys,os,time

from py_ext.lzma import decompress, compress_files
from py_ext.tool import log

path = ''
date = ''

def compress_date(date, path):
    # date: datetime.date(2024, 3, 15)
    package_name = f"raw_{date}.7z"
    package_path = os.path.join(path, package_name)
    if os.path.exists(package_path):
        # 更改文件名称
        package_path.replace('.7z', f'_{time.time()}.7z')

    log(f"压缩打包原始raw文件 > \n{package_path}")

    # 忽略文件夹
    files = []
    for i in os.listdir(path):
        # .7z文件或文件夹 跳过
        if i.endswith(".7z") or not os.path.isfile(os.path.join(path, i)):
            continue

        # 文件时间戳
        t_ms = i.split("_")[-1]

        # 检查时间戳是否在指定日期范围内
        t = datetime.datetime.fromtimestamp(int(t_ms) / 1000)
        if t.date() == date:
            files.append(os.path.join(path, i))

    # 压缩打包
    compress_files(files, package_path, 9)

def keep_compress_date(path):
    # 持久监控压缩日期文件
    while True:
        dates = []
        for f in os.listdir(path):
            filepath = os.path.join(path, f)
            if not os.path.isfile(filepath) or f.endswith(".7z"):
                continue
            
            t = f.split("_")[-1][:-3]
            _date = datetime.datetime.fromtimestamp(int(t)).date()
            if _date not in dates:
                dates.append(_date)

        while len(dates) > 1:
            log(f"开始压缩文件日期 :{dates[0]}")
            compress_date(dates[0], path)
            dates = dates[1:]

        time.sleep(5*60)

if __name__ == "__main__":
    # 获取命令行参数
    if len(sys.argv) not in [2, 3]:
        log("Usage: python compress.py tg_file_path <date>")
        sys.exit(1)

    path = sys.argv[1]

    if len(sys.argv) == 3:
        date = sys.argv[2]
        date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
        compress_date(date, path)
    
    else:
        keep_compress_date(path)
