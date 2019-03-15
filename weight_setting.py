
from pymongo import MongoClient
import pymysql
import pandas as pd
import numpy as np
import time
import datetime
import os
import pytz
import logging
from pytz import timezone, utc
tz = pytz.timezone('Asia/Shanghai')


def custom_time(*args):
    utc_dt = utc.localize(datetime.datetime.utcnow())
    my_tz = timezone("Asia/Shanghai")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()

### 设置logger
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
if os.path.exists(r'./logs') == False:
    os.mkdir('./logs')
    if os.path.exists('./logs/setting_update_log.txt') == False:
        fp = open("./logs/setting_update_log.txt", 'w')
        fp.close()
handler = logging.FileHandler("./logs/setting_update_log.txt", encoding="UTF-8")
handler.setLevel(logging.INFO)
logging.Formatter.converter = custom_time
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class Weight_Setting:

    def __init__(self):
        self.db_host = os.environ['db_host']
        self.db_name = os.environ['db_name']
        self.db_port = int(os.environ['db_port'])
        self.db_user = os.environ['db_user']
        self.db_pwd = os.environ['db_pwd']
        self.db_report_name = os.environ['db_report_name']
        self.mysql_db_host = os.environ['mysql_db_host']
        self.mysql_db_port = int(os.environ['mysql_db_port'])
        self.mysql_db_user = os.environ['mysql_db_user']
        self.mysql_db_pwd = os.environ['mysql_db_pwd']
        self.mysql_db_name = os.environ['mysql_db_name']
        self.cycle_day = int(os.environ['cycle_day'])
        self.mysql_conn = None
        self.mysql_cursor = None
        self.threshold_date = None
        self.interests_attribute = dict()
        self.coordinate_attribute = dict()

    ### mongo数据库的连接
    def mongodb_connection(self):
        client = MongoClient(host=self.db_host, port=self.db_port)
        db = client.get_database(self.db_name)
        db.authenticate(self.db_user.strip(), self.db_pwd.strip())
        self.client = client
        self.db = db

    ### mongodb和mysql数据库的关闭
    def close(self):
        self.client.close()
        self.mysql_cursor.close()
        self.mysql_conn.close()

    ### mysql数据库连接
    def mysql_connection(self):
        self.mysql_conn = pymysql.connect(host=self.mysql_db_host, port=self.mysql_db_port, user=self.mysql_db_user,
                                          passwd=self.mysql_db_pwd, db=self.mysql_db_name, charset='utf8')
        self.mysql_cursor = self.mysql_conn.cursor()

    ### 时间转化
    def string_to_datetime(self, date):
        return datetime.datetime.strptime(date, "%Y-%m-%d")

    ### 找到最近的记录数据
    def find_report_recent_date(self):
        colles_report_date = self.db.get_collection(self.db_report_name).find({}, {'_id': 0, 'cohort_date': 1}) \
            .sort([('cohort_date', -1)]).limit(1)
        if colles_report_date.count() > 0:
            ydate = list(colles_report_date)[0]
            self.threshold_date = str(datetime.datetime.date(self.string_to_datetime(ydate['cohort_date']) +
                                                             datetime.timedelta(days=-self.cycle_day)))
            logger.info('start date:%s' % (self.threshold_date))
            return ydate['cohort_date']

    ### 叠加属性值所产生的数据
    def is_add_attribute(self, iid, report, attribute):
        if iid not in attribute:
            attribute[iid] = {'id': iid, 'install': 0, 'pay': 0, 'cost': 0, 'revenue_day1': 0, 'size': 0}
        attribute[iid]['install'] += report['install']/report['size']
        attribute[iid]['pay'] += report['pay']/report['size']
        attribute[iid]['cost'] += report['cost']/report['size']
        attribute[iid]['revenue_day1'] += report['revenue_day1']/report['size']
        attribute[iid]['size'] += 1

    ### 通过report表中获取的ad_id进行广告的关联
    def find_ads(self, report_ads=None):
        adids = list(report_ads.keys())
        colles_ads = self.db.ads.find({'ad_id': {'$in': adids}}, {'_id': 0, 'ad_id': 1,
                                                                  'pt.adset_spec.targeting.interests': 1,
                                                                  'pt.adset_spec.targeting.geo_locations': 1})
        for ads in colles_ads:
            ad_id = ads['ad_id']
            pt = ads['pt']
            if pt.get('adset_spec') and pt['adset_spec'].get('targeting'):
                if pt['adset_spec']['targeting'].get('interests'):
                    interests = pt['adset_spec']['targeting']['interests']
                    if isinstance(interests, list):
                        for interest in interests:
                            self.is_add_attribute(interest['id'], report_ads[ad_id], self.interests_attribute)
                    elif isinstance(interests, dict):
                        for interest in interests.values():
                            self.is_add_attribute(interest['id'], report_ads[ad_id], self.interests_attribute)
                if pt['adset_spec']['targeting'].get('geo_locations') and \
                        pt['adset_spec']['targeting']['geo_locations'].get('custom_locations'):
                    custom_locations = pt['adset_spec']['targeting']['geo_locations']['custom_locations']
                    for cl in custom_locations:
                        if isinstance(cl, dict):
                            gid = cl['latitude']+'_'+cl['longitude']+'_'+cl['radius']
                            self.is_add_attribute(gid, report_ads[ad_id], self.coordinate_attribute)

    ### 根据条件在report表中进行查找
    def find_report(self):
        colles_report = self.db.report.find({'cohort_date': {'$gte': self.threshold_date}}, {'_id': 0, 'ad_id': 1,
                                                                                             'cohort_date': 1,
                                                                                             'install': 1, 'pay': 1,
                                                                                             'cost': 1,
                                                                                             'revenue_day1': 1})
        report_ads = dict()
        logger.info('the size of report:%d' % (colles_report.count()))
        for report in colles_report:
            ad_id = report['ad_id']
            if ad_id not in report:
                report_ads[ad_id] = {'install': 0, 'pay': 0, 'cost': 0, 'revenue_day1': 0, 'size': 0}
            report_ads[ad_id]['install'] += report['install']
            report_ads[ad_id]['pay'] += report['pay']
            report_ads[ad_id]['cost'] += report['cost']
            report_ads[ad_id]['revenue_day1'] += report['revenue_day1']
            report_ads[ad_id]['size'] += 1
        self.find_ads(report_ads)

    ### 计算属性值需要更新的权重
    def calc_weight(self, attribute):
        attribute_df = pd.DataFrame.from_dict(attribute, orient='index').reindex()

        attribute_df['cpi1'] = attribute_df['cost']/(attribute_df['install']+1)
        attribute_df['cpi2'] = attribute_df['cost']/(attribute_df['pay']+1)
        attribute_df['mean_cost'] = attribute_df['cost']/(attribute_df['size']+1)
        attribute_df['mean_revenue'] = attribute_df['revenue_day1']/(attribute_df['size']+1)
        attribute_df['roi'] = attribute_df['revenue_day1']/(attribute_df['cost']+0.0001)
        num = len(attribute_df)
        attribute_df.sort_values(by=['cost'], ascending=False, inplace=True)
        attribute_df['rank_cost'] = np.linspace(1, 0, num)

        del attribute_df['install']
        del attribute_df['pay']
        del attribute_df['size']
        del attribute_df['cost']
        del attribute_df['revenue_day1']

        attribute_df.sort_values(by=['cpi1'], ascending=True, inplace=True)
        attribute_df['rank_cpi1'] = np.linspace(1, 0, num)

        attribute_df.sort_values(by=['cpi2'], ascending=True, inplace=True)
        attribute_df['rank_cpi2'] = np.linspace(1, 0, num)

        attribute_df.sort_values(by=['roi'], ascending=False, inplace=True)
        attribute_df['rank_roi'] = np.linspace(1, 0, num)

        attribute_df['weight'] = (attribute_df['rank_cost'] + attribute_df['rank_cpi1'] +
                                  attribute_df['rank_cpi2'] + attribute_df['rank_roi'])/4

        attribute_df.sort_values(by=['weight'], ascending=False, inplace=True)
        return attribute_df[['id', 'weight']]

    ### 更新interests的权重操作
    def update_insterests(self):
        logger.info('update interests')
        adid_weight = self.calc_weight(self.interests_attribute)
        rate = 0.1
        sql = 'select id,weight from dw_dim_interest where id in %s' % str(tuple(adid_weight['id']))
        dm_interests = pd.read_sql(sql, self.mysql_conn)
        sql = 'update dw_dim_interest set weight=%f where id=%d'
        dm_interests.columns = ['id', 'update_weight']
        dw_interests = pd.merge(adid_weight, dm_interests, on=['id'], how='inner')
        logger.info('the size of updating interest:%d' % (len(dw_interests)))
        for index in range(len(dw_interests)):
            row = dw_interests.iloc[index]
            tmp = row['update_weight'] + rate * (row['update_weight'] - float(row['weight']))
            if tmp > 1:
                tmp = 1
            if tmp < 0:
                tmp = 0
            self.mysql_cursor.execute(sql % (tmp, row['id']))
            self.mysql_conn.commit()


    ### 更新coordinate的权重操作
    def update_coordinate(self):
        logger.info('update coordinate')
        adid_weight = self.calc_weight(self.coordinate_attribute)
        rate = 0.1
        sql = 'select pid,weight,id from(select pid,weight,concat(latitude,\'_\',longitude,\'_\',radius) as id '\
              'from dw_dim_coordinate)a where  a.id in %s' % str(tuple(adid_weight['id']))
        dm_coordinate = pd.read_sql(sql, self.mysql_conn)
        sql = 'update dw_dim_coordinate set weight=%f where pid=%d'
        adid_weight.columns = ['id', 'update_weight']
        dw_coordinate = pd.merge(adid_weight, dm_coordinate, on=['id'], how='inner')
        logger.info('the size of updating coordinate:%d' % (len(dw_coordinate)))
        for index in range(len(dw_coordinate)):
            row = dw_coordinate.iloc[index]
            tmp = row['update_weight'] + rate * (row['update_weight'] - float(row['weight']))
            if tmp > 1:
                tmp = 1
            if tmp < 0:
                tmp = 0
            self.mysql_cursor.execute(sql % (tmp, row['pid']))
            self.mysql_conn.commit()


    ### 更新两个属性值调用函数
    def update_mysql(self):
        self.update_insterests()
        self.update_coordinate()

    ### 读取本地保留一个更新的日期
    def is_updating(self, recent_date):
        if os.path.exists('./logs/tmp_date.txt'):
            with open('./logs/tmp_date.txt', 'r') as f:
                date = f.readline()
                if date >= recent_date:
                    return False
                else:
                    return True
        else:
            return True

    ### 更新完保留更新的日期
    def save_today(self, date):
        with open('./logs/tmp_date.txt', 'w') as f:
            f.write(date)

    def tmain(self):
        t = time.time()
        try:
            self.mongodb_connection()
            self.mysql_connection()
            ydate = self.find_report_recent_date()
            logger.info('recent date: %s ' % (ydate))
            if self.is_updating(ydate):
                self.find_report()
                self.update_mysql()
                self.save_today(ydate)
                tt = time.time()
                logger.info('it has been updating! it cost %f seconds' % (tt-t))
            else:
                tt = time.time()
                logger.info('Fail,it has been already updated before! it cost %f seconds' % (tt-t))
            self.close()
        except Exception as e:
            logger.info('Fail, Fail, Fail. it is a anomaly problem! %s' % (str(e)))
            pass


def wsmain():
    while True:
        print('start')
        ws = Weight_Setting()
        ws.tmain()
        time.sleep(7*24*60*60)


if __name__ == '__main__':
    wsmain()
