#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    Talk is cheap, show your code!
"""
import os
import sys
import yaml
import argparse
import logging
from datetime import datetime
import confluent_kafka

# TOFIX
class Logger(object):
    """
    log class
    """
    def __init__(self, log_file):
        """
        init logger
        """
        
        self.logger = logging.getLogger()  
        # self.handler = logging.FileHandler(log_file)  
        self.handler = logging.StreamHandler(sys.stdout)
        fmt = '[%(levelname)s]\t[%(asctime)s]\t[%(thread)d]\t' + \
                '[%(filename)s:%(lineno)s]\t[%(message)s]'    
        formatter = logging.Formatter(fmt)   
        self.handler.setFormatter(formatter)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.NOTSET) 


class Producer(object):
    def __init__(self, conf_path):
        self.conf_path = conf_path
        self.load_conf()
        self.init_producer()

    def load_conf(self):
        if not os.path.isfile(self.conf_path):
            sys.exit('Got a wrong conf_file')
        with open(self.conf_path) as f:
            self.conf = yaml.safe_load(f.read())
            print self.conf

    def init_producer(self):
        self.topic = self.conf.get('kafka_topic')
        self.conn_info = self.conf.get('kafka_conn')
        self.p = confluent_kafka.Producer(**self.conn_info)
    
    
    def delivery_report(self, err, msg):
        if err:
            logger.logger.warn('Message failed delivery: %s\n' % err)
        else:
            print msg
            print dir(msg)
            logger.logger.info('Message delivered to %s [%d]\n' % \
                             (msg.topic(), msg.partition()))


    def send(self, value, key='default_key'):
        logger.logger.info('Will send value: %s' % value)

        msg = dict( 
            key=key, 
            value=value, 
        )
        self.p.produce(self.topic, callback=self.delivery_report, **msg)
        self.p.poll(0)
        self.p.flush()

        logger.logger.info('Sendsuccess!')
        

class SqlBuilder(object):


    template = 'insert into t_unibot_status values {instance_id}, {bot_id}, \
                {bot_model_version!r}, {bot_status!r}, {create_time}, {description}'

    def __init__(self):
        pass


    def build(self, params):
        """
        create table t_unibot_status(id int(12) not null primary key auto_increment, instance_id int(12) not null, bot_id int(12) not null, 
        bot_model_version int(12) not null, bot_status varchar(32) not null, cmd_time timestamp(6) not null default CURRENT_TIMESTAMP(6), 
        exe_time timestamp(6) not null default CURRENT_TIMESTAMP(6) on update CURRENT_TIMESTAMP(6), description varchar(100)); 
        
        t_unibot_status
        id, instance_id, bot_id, bot_model_version, bot_status, create_time, update_time, description
        """
        
        checked_ok, params = self.check_and_fix(params)
        if not checked_ok:
            return None

        sql = self.template.format(**params)
        logger.logger.info('Build sql: %s' % sql)
        
        return sql

    def check_and_fix(self, params):
        """
        {"bot_status":"loading","instance_id":1137}
        {"bot_id":2148,"bot_model_version":3,"bot_status":"running","instance_id":1137}
        """
        
        logger.logger.info('Before checking and fixing params: ' + str(params))

        if 'instance_id' not in params:
            logger.logger.warn('Params illegal, instance_id is not exist !')
            return False, params

        if 'bot_status' not in params:
            logger.logger.warn('Params illegal, bot_status is not exist !')
            return False, params

        if 'bot_id' not in params:
            params['bot_id'] = 0
        if 'bot_model_version' not in params:
            params['bot_model_version'] = 3
        time_format = '%Y-%m-%d %H:%M:%S.%f'
        if 'create_time' not in params:
            params['create_time'] = datetime.now().strftime(time_format)
        if 'description' not in params:
            params['description'] = 'no need desc!'
        logger.logger.info('After fixing: ')
        logger.logger.info(params)

        return True, params


def SendSql():
    conf_path = 'producer_conf.yaml'
    sql_builder = SqlBuilder()
    sql_sender = Producer(conf_path)

    def bulid_and_send(params):
        # dict::params
        sql = sql_builder.build(params)
        if sql is not None:
            logger.logger.info('Will send sql: %s' % sql)
            sql_sender.send(sql)
            logger.logger.info('Send sql success!')
        
    return bulid_and_send


if __name__ == '__main__':
    logger = Logger('local_db_svc.log')

    sendsql = SendSql()


    test_cases = [
        {"bot_status":"loading","instance_id":1137},
        {"bot_id":2148,"bot_model_version":3,"bot_status":"running","instance_id":1137}
    ]

    map(sendsql, test_cases)
