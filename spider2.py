import argparse
import multiprocessing
import aiohttp
import asyncio
from lxml import etree
import datetime
from until import logs, Mongo


class Request(object):
    def __init__(self, start_url, log, deep, keywords=None, concurrency_number=1):
        self.url_srt = set()
        self.CONCURRENCY = 100
        self.semaphore = asyncio.Semaphore(self.CONCURRENCY)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36'}
        self.start_url = start_url
        self.concurrency_number = int(concurrency_number)
        self.deep = int(deep)
        self.all_count = 0
        self.queue = multiprocessing.Queue()
        self.log = log
        self.crawl_count = 0
        self.keywords = keywords
        self.conserve_count = 0
        self.Mongo = Mongo(self.log)

    async def analysis(self, html, url, deep):
        # 判断传入页面下一级深度，如果下一级大于设定的深度就不做解析
        if deep > self.deep:
            return
        try:
            html_obj = etree.HTML(html)
            a_list = html_obj.xpath('//a')
            for a_list_item in a_list:
                xpath_url = a_list_item.xpath('./@href')
                if xpath_url:
                    # 设置过滤条件，可设为变量，也可以不设置
                    if 'http' in xpath_url[0] and 'xinhuanet' in xpath_url[0]:
                        # 重复的url就不进入队列了，这步可以采用redis
                        if xpath_url[0] not in self.url_srt:
                            self.url_srt.add(xpath_url[0])
                            self.all_count += 1
                            # self.redis.con.sadd('xinhuanet:get_url_no_crawler', xpath_url[0])
                            self.queue.put({'deep': deep, 'data': xpath_url})
        except:
            self.log.error('解析网页出错,该网页为:' + url)


    async def request(self, session, url):
        try:
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    return await response.text(), response.url
        except:
            print('出错了')
            return '', ''

    async def start_crawl(self, i):
        # 设置超时时间
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # 使用信号量
            async with self.semaphore:
                # 请求首页
                html, response_url = await self.request(session, self.start_url)
                # 解析
                await self.analysis(html, response_url, 1)
                while True:
                    # 判断队列是否为空，为空就退出
                    if self.queue.empty():
                        break
                    # 从队列中取数据
                    data_dir = self.queue.get()
                    # 获取层级和url
                    now_deep = data_dir.get('deep')
                    url_list = data_dir.get('data')
                    for url in url_list:
                        # 抓取数据
                        response_text, response_url = await self.request(session, url)
                        self.crawl_count += 1
                        self.log.info('id:' + str(i) + '抓取成功：' + url)
                        # 保存数据
                        await self.to_mongo(response_text, response_url)
                        # 解析数据
                        await self.analysis(response_text, response_url, now_deep + 1)
                self.log.info('完成')

                # return None

    async def main(self):
        # 创建任务实例，获取抓取信息
        asyncio.create_task(self.get_info())
        # 启动多个抓取任务
        await asyncio.gather(*[self.start_crawl(i) for i in range(self.concurrency_number)])

    def start(self):
        asyncio.run(self.main())

    async def to_mongo(self, html, url):
        if self.keywords:
            if self.keywords in html:
                # 保存网页信息
                self.Mongo.insert({'html': html, 'url': url})
                self.conserve_count += 1
        else:
            self.Mongo.insert({'html':html, 'url': url})
            self.conserve_count += 1

    async def get_info(self):
        while True:
            print('已经抓取：', self.crawl_count, '已经保存：', self.conserve_count, '总数：', self.all_count, datetime.datetime.now())
            if self.conserve_count == self.all_count and self.all_count != 0:
                break
            await asyncio.sleep(10)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # 获取参数
    parser.add_argument("-u", default='')
    parser.add_argument("-d")
    parser.add_argument("-f")
    parser.add_argument("-l", default='1', choices=[str(x) for x in range(1, 6)])
    parser.add_argument("-concurrency", default='1')
    parser.add_argument("-key", default=None, action="store_true", help="active log info.")
    args = parser.parse_args()
    # 创建日志对象
    logger = logs(path=args.f, levels=args.l)
    # 创建爬虫对象
    re = Request(log=logger, start_url=args.u, deep=args.d, keywords=args.key, concurrency_number=args.concurrency)
    re.start()

