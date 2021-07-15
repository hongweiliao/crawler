import multiprocessing
import threading
from until import logs, Request_class, Mongo, Redis
from lxml import etree
import time
import argparse


class Spider:
    def __init__(self, url, log, deep, keywords=None, concurrency_number=1):
        self.start_url = url
        self.log = log
        self.request = Request_class(self.log)
        self.deep = deep
        self.now_deep = 0
        self.keywords = keywords
        self.concurrency_number = concurrency_number
        self.Mongo = Mongo(self.log)
        self.crawl_count = 0
        self.conserve_count = 0
        self.all_count = 1
        self.redis = Redis()
        self.queue = multiprocessing.Queue()

    def analysis(self, html, url, deep):
        url_list = []
        if deep > self.deep:
            return
        try:
            html_obj = etree.HTML(html)
            a_list = html_obj.xpath('//a')
            for a_list_item in a_list:
                xpath_url = a_list_item.xpath('./@href')
                if xpath_url:
                    if 'http' in xpath_url[0] and 'xinhuanet' in xpath_url[0]:
                        self.all_count += 1
                        self.redis.con.sadd('xinhuanet:get_url_no_crawler', xpath_url[0])
                        self.queue.put({'deep': deep, 'data': xpath_url})
        except:
            self.log.error('解析网页出错,该网页为:' + url)
        return url_list

    def to_mongo(self, html, url):
        if self.keywords:
            if self.keywords in html:
                # 保存网页信息
                self.Mongo.insert({'html': html, 'url': url})
                self.conserve_count += 1
        else:
            self.Mongo.insert({'html':html, 'url': url})
            self.conserve_count += 1

    def start(self):
        res = self.request.request_url(self.start_url, request_type='首页', )
        if res:
            self.crawl_count += 1
            self.to_mongo(res.text, res.url)
            if self.deep >= 1:
                self.analysis(res.text, res.url, 1)
                t = []
                for i in range(self.concurrency_number):
                    t.append(threading.Thread(target=self.crawler))
                for item in t:
                    item.start()
                self.get_info()
            else:
                self.log.info('完成')

    def crawler(self):
        while True:
            data_dir = self.queue.get()
            if not data_dir:
                break
            now_deep = data_dir.get('deep')
            url_list = data_dir.get('data')
            for url in url_list:
                res = self.request.request_url(url, request_type='-')
                if res:
                    self.crawl_count += 1
                    self.log.info('抓取成功：'+url)
                    self.redis.con.srem('xinhuanet:get_url_no_crawler', url)
                    self.to_mongo(res.text, res.url)
                    self.analysis(res.text, res.url, now_deep+1)
        self.log.info('完成')


    def get_info(self):
        while True:
            print('已经抓取：', self.crawl_count, '已经保存：', self.conserve_count, '总数：', self.all_count)
            if self.queue.empty():
                time.sleep(30)
                break
            time.sleep(10)


if __name__ == '__main__':
    # python spider.py -u http://www.xinhuanet.com/ -d 1 -f F:\log.txt -l 2
    # 获取参数
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", default='')
    parser.add_argument("-d")
    parser.add_argument("-f")
    parser.add_argument("-l", default='1', choices=[str(x) for x in range(1, 6)])
    parser.add_argument("-concurrency", default='1')
    parser.add_argument("-key", default=None, action="store_true", help="active log info.")
    args = parser.parse_args()
    # 创建日志对象
    logger = logs(path=args.f, levels=args.l)
    s = Spider(args.u, logger, int(args.d), concurrency_number=int(args.concurrency), keywords=args.key)
    s.start()


