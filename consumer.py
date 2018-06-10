﻿#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
    Example of high-level Kafka 0.10 balanced consumer
"""
import os
import sys
import yaml
import collections
import logging.config
import confluent_kafka
import mysql.connector as DB
from pprint import pprint


def setup_logging(cfg_path='logging.yaml', level=logging.INFO, env_key='LOG_CFG'):
    """Setup the logging configuration"""
    path = os.getenv(env_key) or cfg_path
    if os.path.exists(path):
        with open(path) as f:
            cfg = yaml.safe_load(f.read())
        logging.config.dictConfig(cfg)
    else:
        logging.basicConfig(level=level)

setup_logging()
logger = logging.getLogger('dev')

def test():
    with open('consumer_conf.yaml') as f:
        info = yaml.safe_load(f.read())
        conf = info.get('kafka_info')
        conn = conf.get('conn')
        topics = conf.get('topics')

    group = topic.split('__')[0] + '_unibot_status_test'
    conn['group.id'] = group
    c = confluent_kafka.Consumer(**conn)
    c.subscribe(stopics)

    numOfRecords = 2

    try:
        while numOfRecords > 0:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code(
                ) == confluent_kafka.KafkaError._PARTITION_EOF:
                    sys.stderr.write(
                        '%s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise confluent_kafka.KafkaException(msg.error())
            else:
                print msg
                print dir(msg)
                sys.stderr.write('parition: %d, offset: %d, message: %s\n' %
                                    (msg.partition(), msg.offset(), msg.value()))
            numOfRecords = numOfRecords - 1
    except KeyboardInterrupt:
        sys.stderr.write('Aborted by user\n')

    # Close down consumer to commit final offsets.
    c.close()


class ObjectDict(dict):
    def __inti__(self, *args, **kwargs):
        super(ObjectDict, self).__init__(*args, **kwargs)

    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, collections.Mapping):
            value = self.__class__(value)
        return value


class ConfigHandler(object):
    def __init__(self, conf_path):
        self._path = conf_path
        self.loads()

    def __iter__(self):
        for attr in self.__dict__:
            if attr.startswith('_'):
                continue
            value = self.__dict__[attr]
            if isinstance(value, collections.Mapping):
                value = ObjectDict(value)
            yield value

    @property
    def _path(self):
        return self._conf_path

    @_path.setter
    def _path(self, conf_path):
        print conf_path
        if not os.path.isfile(conf_path):
            raise IOError, 'The specified %s is not exists!' % conf_path
        self._conf_path = conf_path

    def loads(self):
        with open(self._path) as f:
            conf = yaml.safe_load(f.read())
        if not isinstance(conf, collections.Mapping):
            raise TypeError, 'Conf should be a dict'
        self.extract_cfg(conf)

    def extract_cfg(self, conf):
        for k, v in conf.items():
            if isinstance(v, collections.Mapping):
                v = ObjectDict(v)
            self.__dict__[k] = v


class DBHandler(object):
    """
    DB class
    """
    
    def __init__(self, conf):
        """
        init db
        """
        # try:
        #     self.conn = MySQLdb.connect(conf.db_host, conf.db_user, 
        #             conf.db_pass, conf.db_name, charset = 'utf8')
        #     self.cursor = self.conn.cursor()
            
        #     logger.info('DB params: ' + conf.db_host + ' ' + conf.db_user + ' ' + \
        #             conf.db_pass + ' ' + conf.db_name)
        #     logger.info('Connect DB successed !')
        # except:
        #     logger.error('Connect DB error !')

        try:
            self.conn = DB.connect(**conf)
            self.cursor = self.conn.cursor
            logger.info('DB params: ' + ';'.join(map(lambda k_v: '%s: %s' % k_v, conf.items())))
            logger.info('Connect DB successed!')
        except DB.Error as err:
            logger.error('Connect DB Error: ' + err)
    
    # def exe(self, sql):
        # """
        # sql = 'insert into table_name values (%s, %s, ...)' %  xxx, xxx, ...
        # """
        # # sql = 'desc t_unibot_status;'
        # try:
        #     self.cursor.execute(sql)
        #     self.conn.commit()
        # except:
        #     self.conn.rollback()
        
        # ret = ""
        # try:
        #     ret = self.cursor.fetchall()
        #     logger.info('Fetch db result successed: ')
        #     logger.info(ret)
        # except:
        #     logger.warn('Fetch db result failed: ')
        #     logger.warn(ret)

    def exe_many(self, sql, params_seq):
        self.cursor.execute_many(sql, params_seq)
        
    def close(self):
        """
        close db
        """
        try:
            self.cursor.close()
            self.conn.close()
            logger.info("Close DB successed !")
        except:
            logger.warn('Close DB failed !')


class SqlBuilder(object):

    query = 'insert into t_unibot_status values {instance_id}, {bot_id}, \
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
        logger.info('Build sql: %s' % sql)
        
        return sql

    def check_and_fix(self, params):
        """
        {"bot_status":"loading","instance_id":1137}
        {"bot_id":2148,"bot_model_version":3,"bot_status":"running","instance_id":1137}
        """
        
        logger.info('Before checking and fixing params: ' + str(params))

        if 'instance_id' not in params:
            logger.warn('Params illegal, instance_id is not exist !')
            return False, params

        if 'bot_status' not in params:
            logger.warn('Params illegal, bot_status is not exist !')
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
        logger.info('After fixing: ')
        logger.info(params)

        return True, params


class Consumer(object):
    def __init__(self, conf):
        self.conf = conf
        self.init_kafka()
        self.init_mysql_executer()


    def init_kafka(self):
        conn_info = self.conf.conn
        self.c = confluent_kafka.Consumer(**conn_info)

        topics = self.conf.topics
        self.c.subscribe(topics)

    def init_mysql_executer(self):
        pass

    def consume(self):
        while True:
            msg = self.c.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                yield msg
            else:
                self.handler.exe(msg)

    def consume_many():
        pass
            
    def set_mysql_handler(self, handler):
        self.mysql_handler = handler

    def test(self):
        print '0'
        numOfRecords = 2
        try:
            while numOfRecords > 0:
                msg = self.c.poll(timeout=1.0)
                if msg is None:
                    print 'msg is none'
                    continue
                if msg.error():
                    if msg.error().code(
                    ) == confluent_kafka.KafkaError._PARTITION_EOF:
                        sys.stderr.write(
                            '%s [%d] reached end at offset %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise confluent_kafka.KafkaException(msg.error())
                else:
                    print msg
                    print dir(msg)
                    sys.stderr.write('parition: %d, offset: %d, message: %s\n' %
                                        (msg.partition(), msg.offset(), msg.value()))
                numOfRecords = numOfRecords - 1
        except KeyboardInterrupt:
            sys.stderr.write('Aborted by user\n')

        # Close down consumer to commit final offsets.
        self.c.close()
        

def SqlConsumer(conf_path):
    conf = ConfigHandler(conf_path)
    consumer = Consumer(conf.kafka_info)
    db = DBHandelr(conf.mysql_info)

    def consume_and_exc():
        pass

    return consume_and_exc

def test_confighandler():


    cfg_consumer = 'consumer.yaml'
    conf = ConfigHandler(cfg_consumer)
    db = DBHandler(conf.mysql_info)
    # test if conf is a iterator
    for k in conf:
        print 'k :\n %s' % k

def test_logging():
    setup_logging()
    logger = logging.getLogger('dev')
    logger.info('Hello INFO!')

if __name__ == '__main__':
    # test()
    # c = Consumer('consumer.yaml')
    # c.test()
    # test_logging()
    test_confighandler()



