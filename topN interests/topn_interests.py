from pymongo import MongoClient
import pymysql
import pandas as pd
import numpy as np
import os

db_host = os.environ['db_host']
db_name = os.environ['db_name']
db_port = int(os.environ['db_port'])
db_user = os.environ['db_user']
db_pwd = os.environ['db_pwd']
client = MongoClient(db_host, db_port, maxPoolSize=200)
db = client.get_database(db_name)
db.authenticate(db_user, db_pwd)

mysql_db_host = os.environ['mysql_db_host']
mysql_db_port = os.environ['mysql_db_port']
mysql_db_user = os.environ['mysql_db_user']
mysql_db_pwd = os.environ['mysql_db_pwd']
mysql_db_name = os.environ['mysql_db_name']
conn = pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_pwd, db=mysql_db_name,
                       port=mysql_db_port)


def find_evaluation():
    colles_evaluation = db.evaluation.find({'pt': {'$ne': None}}, {'_id': 0, 'lifetime_install': 1, 'lifetime_pay': 1,
                                                                   'lifetime_spend': 1, 'pt.adset_spec.targeting.interests': 1}).batch_size(1)
    evaluation = dict()
    index = 0
    for eva in colles_evaluation:
        index += 1
        print(index)
        try:
            if 'pt' in eva and 'adset_spec' in eva['pt'] and 'targeting' in eva['pt']['adset_spec'] and \
               'interests' in eva['pt']['adset_spec']['targeting']:
                interests = eva['pt']['adset_spec']['targeting']['interests']
                if isinstance(interests, list):
                    for interest in interests:
                        if interest['id'] not in evaluation:
                            evaluation[interest['id']] = {'id': interest['id'], 'name': interest['name'],
                                                          'lifetime_install': 0, 'lifetime_pay': 0, 'lifetime_spend': 0
                                                          }
                        evaluation[interest['id']]['lifetime_install'] += eva['lifetime_install']
                        evaluation[interest['id']]['lifetime_pay'] += eva['lifetime_pay']
                        evaluation[interest['id']]['lifetime_spend'] += float(eva['lifetime_spend'])

                elif isinstance(interests, dict):
                    for interest in interests.values():
                        if interest['id'] not in evaluation:
                            evaluation[interest['id']] = {'id': interest['id'], 'name': interest['name'],
                                                          'lifetime_install': 0, 'lifetime_pay': 0, 'lifetime_spend': 0
                                                          }
                            evaluation[interest['id']]['lifetime_install'] += eva['lifetime_install']
                            evaluation[interest['id']]['lifetime_pay'] += eva['lifetime_pay']
                            evaluation[interest['id']]['lifetime_spend'] += float(eva['lifetime_spend'])
        except:
            pass
    print(evaluation)
    # print(colles_evaluation)
    return evaluation


def insert_dw_dim_interest_sample(evaluation_df):
    cursor = conn.cursor()
    topN = 1000
    num = len(evaluation_df)
    for index in range(num):
        if index < topN:
            try:
                row = evaluation_df.iloc[index]
                sql = "INSERT INTO dw_dim_interest_sample(id,name) values ({0},'{1}')".format(row['id'], row['name'])
                print(sql)
                cursor.execute(sql)
                conn.commit()
            except:
                pass
        else:
            break


def get_topN_interests(evaluation):
    evaluation_df = pd.DataFrame.from_dict(evaluation, orient='index').reindex()
    evaluation_df['cpp'] = evaluation_df['lifetime_spend']/(evaluation_df['lifetime_pay']+0.000001)
    evaluation_df['cpi'] = evaluation_df['lifetime_spend']/(evaluation_df['lifetime_install']+0.000001)
    num = len(evaluation_df)
    evaluation_df.sort_values(by=['cpp'], ascending=True, inplace=True)
    evaluation_df['rank_cpp'] = np.linspace(1, 0, num)
    evaluation_df.sort_values(by=['cpi'], ascending=True, inplace=True)
    evaluation_df['rank_cpi'] = np.linspace(1, 0, num)
    evaluation_df['rank'] = 0.4*evaluation_df['rank_cpi']+0.6*evaluation_df['rank_cpp']
    evaluation_df.sort_values(by=['cpi'], ascending=True, inplace=True)
    insert_dw_dim_interest_sample(evaluation_df)


if __name__ == '__main__':
    get_topN_interests(find_evaluation())