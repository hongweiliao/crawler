import logging
import os
import logging.handlers
import setting
import pymongo
import requests
import time
import redis


# 日志类
class logs:
    def __init__(self, path=None, levels='1'):
        self.logger = logging.getLogger("")
        self.logger.handlers.clear()
        # 设置输出的等级
        levels_dir = {
            '1': logging.DEBUG,
            '2': logging.INFO,
            '3': logging.WARNING,
            '4': logging.ERROR,
            '5': logging.CRITICAL}
        if not path:
            # 创建文件目录
            logs_dir = "log"
            if os.path.exists(logs_dir) and os.path.isdir(logs_dir):
                pass
            else:
                os.mkdir(logs_dir)
            # 修改log保存位置
            # timestamp = time.strftime("%Y-%m-%d", time.localtime())
            logfilename = 'spider.log'
            logfilepath = os.path.join(logs_dir, logfilename)
        else:
            logfilepath = path
        rotatingFileHandler = logging.handlers.RotatingFileHandler(filename=logfilepath,
                                                                   maxBytes=1024 * 1024 * 50,
                                                                   backupCount=5)
        # 设置输出格式
        formatter = logging.Formatter("%(asctime)s - %(threadName)s - %(levelname)s: %(message)s")
        rotatingFileHandler.setFormatter(formatter)
        # 控制台句柄
        # console = logging.StreamHandler()
        # console.setLevel(logging.NOTSET)
        # console.setFormatter(formatter)
        # 添加内容到日志句柄中

        self.logger.addHandler(rotatingFileHandler)
        # self.logger.addHandler(console)
        self.logger.setLevel(levels_dir.get(levels))

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)


# 操作请求的类
class Request_class:
    headers_no_cookie = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
        'cache-control': 'max-age=0',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'
    }
    ip = ''
    timeout_times = 0

    def __init__(self, log):
        self.log = log
        self.redis = Redis()
        self.request = requests.session()

    def request_url(self, url, request_type, headers=None):
        while True:
            try:
                if not self.redis.con.sismember('get_set', url):
                    # res = requests.get(url, headers=self.headers_no_cookie, timeout=10, proxies=self.ip)
                    if not headers:
                        res = self.request.get(url, headers=self.headers_no_cookie, timeout=30)
                    else:
                        res = self.request.get(url, headers=headers, timeout=30)
                    if res.status_code == 200:
                        self.timeout_times = 0
                        self.redis.con.sadd('get_set', url)
                        return res
                    # 处理异常
                    else:
                        if res.status_code == 404:
                            return None
                        else:
                            return None
                else:
                    return None
            except Exception as e:
                self.log.info('请求%s出错，重新请求, err:%s' % (request_type, str(e)))
                if 'Cannot connect to proxy' in str(e):
                    self.log.info('更换ip')
                if 'Read timed out.' in str(e):
                    self.timeout_times += 1
                    if self.timeout_times == 5:
                        self.timeout_times = 0
                        self.log.info('更换ip')

    def request_post_url(self, url, data, request_type, post_headers=None):
        while True:
            try:
                if not self.redis.con.sismember('post_set', url + str(data)):
                    # res = requests.get(url, headers=self.headers_no_cookie, timeout=10, proxies=self.ip)
                    if not post_headers:
                        res = self.request.post(url, headers=self.headers_no_cookie, timeout=30, data=data, verify=False)
                    else:
                        res = self.request.post(url, headers=post_headers, timeout=30, data=data, verify=False)
                    if res.status_code == 200:
                        self.timeout_times = 0
                        self.redis.con.sadd('post_set', url)
                        return res

                    # 处理异常
                    else:
                        if res.status_code == 404:
                            return None
                        elif res.status_code == 401:
                            return None
                        else:
                            return None
                else:
                    return None
            except Exception as e:
                self.log.info('请求%s出错，重新请求, err:%s' % (request_type, str(e)))
                if 'Cannot connect to proxy' in str(e):
                    self.log.info('更换ip')
                if 'Read timed out.' in str(e):
                    self.timeout_times += 1
                    if self.timeout_times == 5:
                        self.timeout_times = 0
                        self.log.info('更换ip')


# 操作mongo的类
class Mongo:

    def __init__(self, log):
        self.client = pymongo.MongoClient(setting.MONGO_URL)
        self.db = self.client[setting.MONGO_DB]
        self.cool = self.db[setting.MONGO_COOL]
        self.log = log

    def insert(self, data):
        if type(data) == dict:
            data['spider_time'] = time.time()
            self.cool.insert(data)
        else:
            log.error('数据格式有误，无法插入数据库')


# 操作redis的类
class Redis:
    def __init__(self):
        self.con = redis.Redis(host=setting.REDIS_HOST, port=setting.REDIS_PORT, decode_responses=True, db=setting.REDIS_DB)
