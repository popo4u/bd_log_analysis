#!/usr/bin/python
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
import simplejson as josn
import mysql.connector as DB




# import gevent
# from gevent import monkey; monkey.patch_all()

import threading

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


class ObjectDict(dict):
    def __inti__(self, *args, **kwargs):
        super(ObjectDict, self).__init__(*args, **kwargs)

    def __getattr__(self, name):
        value = self[name]
        if isinstance(value, collections.Mapping):
            value = self.__class__(value)
        return value


class ConfigHandler(object):
    def __init__(self, cfg_path):
        self._path = cfg_path
        self.loads()

    def __iter__(self):
        return self._cfg.__iter__()

    def __getattr__(self, name):
        value = self._cfg[name]
        if isinstance(value, collections.Mapping):
            value = ObjectDict(value)
        return value

    def __getitem__(self, key):
        return self._cfg.__getitem__(key)

    @property
    def _path(self):
        return self._cfg_path

    @_path.setter
    def _path(self, cfg_path):
        if not os.path.isfile(cfg_path):
            raise IOError, 'The specified path(%s) is not exists!' % cfg_path
        self._cfg_path = cfg_path

    @property
    def _ori_data():
        return self._cfg

    @_ori_data.setter
    def _ori_data(self, data):
        if not isinstance(data, collections.Mapping):
            raise TypeError, 'Data from file %s should be a dict' % self._cfg_path
        self._cfg = data

    def loads(self):
        with open(self._path) as f:
            cfg = yaml.safe_load(f.read())
        self._ori_data = cfg


class DBHandler(object):
    
    def __init__(self, conf):
        try:
            self.conn = DB.connect(**conf)
            self.cursor = self.conn.cursor()
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
        self.cursor.executemany(sql, params_seq)

    def exe(self, sql):
        print 'got sql %s' % sql
        # self.cursor.execute(sql)
        
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
    '''
    insert into table_name (col1, col2, col3) values (val1, val2, val3)
    '''

    params = ('instance_id', 'bot_id', 'bot_model_version', 'bot_status', 'create_time', 'description')

    QueryParams = collections.namedtuple('QueryParams', params)

    query = ('insert into t_unibot_status '
             '(instance_id, bot_id, bot_model_version, bot_status, create_time, description) bvalues '
             '{instance_id}, {bot_id}, {bot_model_version}, {bot_status}, {create_time}, {description}')

    check_failed_journal = 'Params illegal, %s not exist!, params: \n==> %s'

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

        sql = self.query.format(**params)
        logger.info('Build sql: %s' % sql)
        
        return sql

    def check_and_fix(self, params):
        """
        {"bot_status":"loading","instance_id":1137}
        {"bot_id":2148,"bot_model_version":3,"bot_status":"running","instance_id":1137}
        """

        _params = self.QueryParams._make(params.split(','))
        self.ori_params = copy.deepcopy(_params)

        logger.debug('Before checking and fixing params: \n==> %s', json.dumps(_params))

        if 'instance_id' not in _params:
            self.check_failed('instance_id')
            return False, self.ori_params

        if 'bot_status' not in _params:
            self.check_failed('bot_status')
            return False, self.ori_params

        if 'bot_id' not in _params:
            _params['bot_id'] = 0
        if 'bot_model_version' not in _params:
            _params['bot_model_version'] = 3
        time_format = '%Y-%m-%d %H:%M:%S.%f'
        if 'create_time' not in _params:
            _params['create_time'] = datetime.now().strftime(time_format)
        if 'description' not in _params:
            _params['description'] = 'no need desc!'

        logger.debug('After fixing params: \n==> %s', json.dumps(_params))

        return True, _params

    def check_failed(self, key):
        journal = 'Params illegal, %s not exist!, params: \n==> %s'
        logger.warn(journal, key), json.dumps(self.ori_params)


class Consumer(object):
    def __init__(self, conf):
        self.conf = conf
        self.init_kafka()
        self.sqlbulider = SqlBuilder()
        self.db = DBHandler(conf.mysql_info)

        self.continue_run = True

    def init_kafka(self):
        conn_info = self.conf.kafka_info.conn
        self.c = confluent_kafka.Consumer(**conn_info)

        topics = self.conf.kafka_info.topics
        self.c.subscribe(topics)


    def run(self, _id):
        try:
            while self.continue_run:
                logger.debug('(%s)consumer waiting for msg', _id)
                msg = self.c.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                        logger.info('toppic(%s) [partition(%d)] Reached end at offset %d', msg.topic(), msg.partition(), msg.offset())
                    else:
                        logger.exception('Kafka Error: %s', msg.error())
                    continue
                data = msg.value()
                logger.info(data)
                query = data
                # query = self.sqlbulider.build(data)
                if query is not None:
                    self.db.exe(query)
                    logger.debug('%s exe sql', _id)
        except:
            raise
        finally:
            self.close()

    def consume_many():
        pass


    def terminate(self):
        self.continue_run = False
    
    def close(self):
        self.c.close()
        self.db.close()   

    def test(self):
        numOfRecords = 2
        try:
            while numOfRecords > 0 or 1:
                logger.debug('start fetching msgs')
                msgs = self.c.consume(num_messages=3, timeout=5.0)
                for msg in msgs:
                    logger.debug('got %s msg', len(msgs))
                    if msg is None:
                        logger.debug('msg is none')
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
                        logger.debug('type: %s', type(msg.value()))
                        logger.debug('value: %s' , msg.value())
                        # logger.debug(msg.__dict__)
                        sys.stderr.write('parition: %d, offset: %d, message: %s\n' %
                                            (msg.partition(), msg.offset(), msg.value()))
                numOfRecords = numOfRecords - 1
        except KeyboardInterrupt:
            sys.stderr.write('Aborted by user\n')

        # Close down consumer to commit final offsets.
        self.c.close()
        

def SqlConsumer(cfg):
    # config info

    # fetch data from kafka
    consumer = Consumer(cfg.kafka_info)
    # init mysql
    db = DBHandler(cfg.mysql_info)

    sqlbulider = SqlBuilder()

    def consume_and_exc():
        try:
            while True:
                # list of params_seq
                # data = consumer.consume_many()
                # requerys = map(sqlbulider.build, data)
                # db.exe_many(requerys)
                print '0'
                data = consumer.consume()
                print '2'
                requery = sqlbulider.build(data)
                print '3'
                logger.info('Build MySQL requery: %s', requery)
                # if requery is not None:
                #     db.exe(requery)
        except:
            raise
        finally:
            db.close()
            consumer.close()

    return consume_and_exc


def gevent_run(cfg_path):
    cfg = ConfigHandler(cfg_path)
    consume_and_exc = SqlConsumer(cfg)

    g = gevent.spawn(consume_and_exc)
    g.join()


def run(cfg_path):
    cfg = ConfigHandler(cfg_path)
    consumer_num = cfg.kafka_info.partition_num
    consumers = [ Consumer(cfg) for i in range(consumer_num) ]

    
    # consumer1 = Consumer(cfg)
    # consumer2 = Consumer(cfg)
    # g_1 = gevent.spawn(consumer1.run, 1)
    # g_2 = gevent.spawn(consumer2.run, 2)
    # gevent.joinall([g_1, g_2])
    # gevent.joinall(map(gevent.spawn, [consumer1.run, consumer2.run]))
    try:
        thread_list = []
        for i, c in enumerate(consumers, 1):
            # thread_list.append(threading.Thread(target=c.run, args=(i, )))
            t = threading.Thread(target=c.run, args=(i, ))
            t.daemon = True
            thread_list.append(t)

        map(lambda t: t.start(), thread_list)
        map(lambda t: t.join(), thread_list)
        logger.info('never reach the end of join')
    except KeyboardInterrupt:
        logger.info('this will never happen!')
        map(lambda c: c.terminate(), consumers)
        raise


def test_confighandler():

    cfg_consumer = 'consumer.yaml'
    conf = ConfigHandler(cfg_consumer)
    db = DBHandler(conf.mysql_info)
    # test if conf is a iterator
    for k in conf:
        print 'k :\n %s' % k
    print type(conf['mysql_info']), conf['mysql_info']
    print type(conf.mysql_info), conf.mysql_info

def test_logging():
    setup_logging()
    logger = logging.getLogger('dev')
    logger.info('Hello INFO!')

if __name__ == '__main__':

    cfg_path = 'consumer.yaml'
    cfg = ConfigHandler(cfg_path)
    c = Consumer(cfg)
    c.test()
    # test_logging()
    # test_confighandler()
    # gevent_run(cfg_path)
    # run(cfg_path)






