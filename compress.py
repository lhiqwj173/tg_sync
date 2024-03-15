import datetime

from py_ext.lzma import decompress, compress_files

path = ''
date = ''

def compress_date(date, path):
    # date: datetime.date(2024, 3, 15)
    package_name = f"raw_{date}.7z"
    log(f"压缩打包原始raw文件 > {package_name}")

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
    compress_files(files, os.path.join(path, package_name), 9)


if __name__ == "__main__":
    # 获取命令行参数
    if len(sys.argv) != 3:
        log("Usage: python tg.py tg_path date")
        sys.exit(1)

    path = sys.argv[1]
    date = sys.argv[1]

    date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    compress_date(date, path)