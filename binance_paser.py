"""
格式化binance返回的数据
储存到mongo中
"""
import os, struct,time

class binance_base():
    def __init__(self, file_path):
        self.file_path = file_path
        # 文件大小
        self.bytes = os.path.getsize(file_path) 
        self.readed = 0
        self.f = None

        self.print_count = 1
        self.begin_time = 0

    def _size(self):
        pass

    def _pase_data(self, raw):
        pass
    
    def __len__(self):
        return int(self.bytes / self._size())

    def __iter__(self):
        self.f = open(self.file_path, 'rb')
        return self

    def __next__(self):
        if self.begin_time == 0:
            self.begin_time = time.time()

        if self.readed == self.bytes:
            raise StopIteration

        # 数据大小
        _bytes = self._size()

        # 进度条
        if (self.print_count % 500) == 0:
            all_count = int(self.bytes/_bytes)
            done_count = int(self.readed/_bytes)
            speed = done_count / (time.time() - self.begin_time)
            remain_time = (all_count - done_count) / speed
            print(f"\r{done_count}/{all_count} remain:{remain_time:.2f} sec", end='')
        self.print_count += 1
        self.readed += _bytes

        # 读取数据
        data = self.f.read(_bytes)

        # 解析数据
        return self._pase_data(data)

class trade(binance_base):
    def _size(self):
        return 96

    def _pase_data(self, raw):
        """
        ///////////////////////////////
        // 交易数据
        ///////////////////////////////
        96字节
        8字节对齐
        struct data_trade {
            int type;
            unsigned long long event_timestamp;
            unsigned long long save_timestamp;
            char[10] symbol;
            double price;
            double vol;
            unsigned long id;
            unsigned long buy_id;
            unsigned long sell_id;
            unsigned long long deal_timestamp;
            bool is_buyer_maker;
        };
        """
        data = {}
        data['type'] = struct.unpack('i', raw[:4])[0]
        data['event_timestamp'] = struct.unpack('Q', raw[8:16])[0]
        data['save_timestamp'] = struct.unpack('Q', raw[16:24])[0]
        data['symbol'] = raw[24:40].decode('utf-8').strip('\x00')
        data['price'] = struct.unpack('d', raw[40:48])[0]
        data['vol'] = struct.unpack('d', raw[48:56])[0]
        data['id'] = struct.unpack('Q', raw[56:64])[0]
        data['buy_id'] = struct.unpack('Q', raw[64:72])[0]
        data['sell_id'] = struct.unpack('Q', raw[72:80])[0]
        data['deal_timestamp'] = struct.unpack('Q', raw[80:88])[0]
        data['is_buyer_maker'] = struct.unpack('?', raw[88:89])[0]
        return data

class depth(binance_base):
    def _size(self):
        return 352

    def _pase_data(self, raw):
        """
        ///////////////////////////////
        // 交易数据
        ///////////////////////////////
        352字节
        8字节对齐
        struct data_depth_10
        {
            double bid_price[10] = { 0 };
            double bid_vol[10] = { 0 };
            double ask_price[10] = { 0 };
            double ask_vol[10] = { 0 };
            size_t id;
            symbol_name symbol;
            extra::time::timestamp_ms save_timestamp;
        };
        """
        data = {}
        data['bid1_price'] = struct.unpack('d', raw[:8])[0]
        data['bid2_price'] = struct.unpack('d', raw[8:16])[0]
        data['bid3_price'] = struct.unpack('d', raw[16:24])[0]
        data['bid4_price'] = struct.unpack('d', raw[24:32])[0]
        data['bid5_price'] = struct.unpack('d', raw[32:40])[0]
        data['bid6_price'] = struct.unpack('d', raw[40:48])[0]
        data['bid7_price'] = struct.unpack('d', raw[48:56])[0]
        data['bid8_price'] = struct.unpack('d', raw[56:64])[0]
        data['bid9_price'] = struct.unpack('d', raw[64:72])[0]
        data['bid10_price'] = struct.unpack('d', raw[72:80])[0]

        data['bid1_vol'] = struct.unpack('d', raw[80:88])[0]
        data['bid2_vol'] = struct.unpack('d', raw[88:96])[0]
        data['bid3_vol'] = struct.unpack('d', raw[96:104])[0]
        data['bid4_vol'] = struct.unpack('d', raw[104:112])[0]
        data['bid5_vol'] = struct.unpack('d', raw[112:120])[0]
        data['bid6_vol'] = struct.unpack('d', raw[120:128])[0]
        data['bid7_vol'] = struct.unpack('d', raw[128:136])[0]
        data['bid8_vol'] = struct.unpack('d', raw[136:144])[0]
        data['bid9_vol'] = struct.unpack('d', raw[144:152])[0]
        data['bid10_vol'] = struct.unpack('d', raw[152:160])[0]

        data['ask1_price'] = struct.unpack('d', raw[160:168])[0]
        data['ask2_price'] = struct.unpack('d', raw[168:176])[0]
        data['ask3_price'] = struct.unpack('d', raw[176:184])[0]
        data['ask4_price'] = struct.unpack('d', raw[184:192])[0]
        data['ask5_price'] = struct.unpack('d', raw[192:200])[0]
        data['ask6_price'] = struct.unpack('d', raw[200:208])[0]
        data['ask7_price'] = struct.unpack('d', raw[208:216])[0]
        data['ask8_price'] = struct.unpack('d', raw[216:224])[0]
        data['ask9_price'] = struct.unpack('d', raw[224:232])[0]
        data['ask10_price'] = struct.unpack('d', raw[232:240])[0]

        data['ask1_vol'] = struct.unpack('d', raw[240:248])[0]
        data['ask2_vol'] = struct.unpack('d', raw[248:256])[0]
        data['ask3_vol'] = struct.unpack('d', raw[256:264])[0]
        data['ask4_vol'] = struct.unpack('d', raw[264:272])[0]
        data['ask5_vol'] = struct.unpack('d', raw[272:280])[0]
        data['ask6_vol'] = struct.unpack('d', raw[280:288])[0]
        data['ask7_vol'] = struct.unpack('d', raw[288:296])[0]
        data['ask8_vol'] = struct.unpack('d', raw[296:304])[0]
        data['ask9_vol'] = struct.unpack('d', raw[304:312])[0]
        data['ask10_vol'] = struct.unpack('d', raw[312:320])[0]

        data['id'] = struct.unpack('Q', raw[320:328])[0]
        data['symbol'] = raw[328:344].decode('utf-8').strip('\x00')
        data['save_timestamp'] = struct.unpack('Q', raw[344:352])[0]

        # 返回
        return data


if __name__ == '__main__':

    depth_file = r"C:\Users\lh\Desktop\fsdownload\depth_10_100_1708502752297"
    trade_file = r"C:\Users\lh\Desktop\fsdownload\trade_1708502749235"

    t = trade(trade_file)
    d = depth(depth_file)

    for i in d:
        print(i)