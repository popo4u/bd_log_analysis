#!/usr/bin/env python
# -*- coding:utf-8 -*-


import os
import sys
import yaml
import copy
import json
import numbers
import decimal
import threading
import collections
import logging.config
import dateutil.parser
import confluent_kafka
import mysql.connector as DB
from datetime import datetime
from functools import partial


def setup_logging(cfg_path='logging.yaml', level=logging.INFO):
    """Setup the logging configuration"""
    if os.path.exists(cfg_path):
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f.read())
        logging.config.dictConfig(cfg)
    else:
        logging.basicConfig(level=level)

def object_hook(obj):
    CONVERTERS = {
        'datetime': dateutil.parser.parse,
        'decimal': decimal.Decimal,
    }

    _spec_type = obj.get('_spec_type')
    if not _spec_type:
        return obj
    if _spec_type in CONVERTERS:
        return CONVERTERS[_spec_type](obj['val'])
    else:
        raise Exception('Json load Error: Unknown {}'.format(_spec_type))


class SpecJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return dict(val=obj.isoformat(), _spec_type='datetime')
        elif isinstance(obj, decimal.Decimal):
            return dict(val=str(obj), _spec_type='decimal')
        else:
            return super(SpecJSONEncoder, self).default(obj)


# parse datetime by json
json_dumps = partial(json.dumps, cls=SpecJSONEncoder)
json_loads = partial(json.loads, object_hook=object_hook)

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
            logger.info('DB connect successed!')
        except DB.Error as err:
            logger.error('Connect DB Error: ' + err)
    
    def exe_many(self, sql, params_seq):
        try:
            self.cursor.executemany(sql, params_seq)
            logger.info('DB execute %s sql SUCCESSFUL', len(params_seq))
        except Exception as e:
            logger.exception('DB exception:')
        
    def exe(self, sql, params):
        try:
            self.cursor.execute(sql, params)
            logger.info('DB execute 1 sql SUCCESSFUL')
        except Exception as e:
            logger.exception('DB exception:')

    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
            logger.info("DB close successed !")
        except:
            logger.warn('DB close failed !')


class SqlBuilder(object):
    '''
    insert into table_name (col1, col2, col3) values (val1, val2, val3)
    '''
    query = ("insert into t_unibot_status "
             "(instance_id, bot_id, bot_model_version, "
             "bot_code_version, bot_status, vm_ip, create_time, update_time, description) "
             "values (%(instance_id)s, %(bot_id)s, %(bot_model_version)s, %(bot_code_version)s, "
             "%(bot_status)s, %(vm_ip)s, %(create_time)s, %(update_time)s, %(description)s)")

    def __init__(self, params_info):
        self.params_info = params_info
        self.ime_format = '%Y-%m-%d %H:%M:%S.%f'

    def check_and_fix(self, params_str):
        def is_empty(value):
            if isinstance(value, type(None)):
                return True
            if isinstance(value, (str, unicode)):
                return len(value.strip()) == 0
            if isinstance(value, numbers.Number):
                return False
            return False

        logger.debug('Before checking and fixing params: \n==> %s', params_str)

        params = json_loads(params_str)
        for param, info in self.params_info.items():
            if is_empty(params.get(param)) and info.get('required'):
                logger.warn('Params illegal, %s is required!', param)
                return None
            elif is_empty(params.get(param)):
                value = info.get('default')
                if info.get('default_type') == 'code':
                    value = eval(value)
                params[param] = value
        logger.debug('Params checked and fixed:\n ==> %s', json_dumps(params))
        return params


    def build(self, params_str):
        """
        create table t_unibot_status(id int(12) not null primary key auto_increment, instance_id int(12) not null, bot_id int(12) not null, 
        bot_model_version int(12) not null, bot_status varchar(32) not null, cmd_time timestamp(6) not null default CURRENT_TIMESTAMP(6), 
        exe_time timestamp(6) not null default CURRENT_TIMESTAMP(6) on update CURRENT_TIMESTAMP(6), description varchar(100)); 
        
        t_unibot_status
        (id, )instance_id, bot_id, bot_model_version, bot_status, create_time, update_time, description
        """
        
        params = self.check_and_fix(params_str)
        if not params:
            return None, None

        return self.query, params

    def build_many(self, params_str_seq):
        params_seq = []
        for params_str in params_str_seq:
            if not params_str: continue
            param = self.check_and_fix(params_str)
            if param is None: continue 
            params_seq.append(param)
        return self.query, params_seq


class Consumer(object):
    def __init__(self, conf):
        self.conf = conf
        self.init_kafka()
        self.sqlbulider = SqlBuilder(conf.query_params)
        self.db = DBHandler(conf.mysql_info)

        self.continue_run = True

    def init_kafka(self):
        conn_info = self.conf.kafka_info.conn
        self.c = confluent_kafka.Consumer(**conn_info)

        topics = self.conf.kafka_info.topics
        self.c.subscribe(topics)

    def extract_info(self, msg):
        return msg.value().split('\t')[-1]

    def consume(self, _id):
        while self.continue_run:
            try:
                logger.debug('(%s)consumer waiting for msg', _id)
                msgs = self.c.consume(num_messages=100, timeout=3.0)
                logger.debug('(%s)consumer got %s msgs', _id, len(msgs))
                for msg in msgs:
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                            logger.info('toppic(%s) [partition(%d)] Reached end at offset %d', 
                                        msg.topic(), msg.partition(), msg.offset())
                        else:
                            logger.exception('Kafka Error: %s', msg.error())
                        continue
                    query, params = self.sqlbulider.build(self.extract_info(msg))
                    if params is not None:
                        self.db.exe(query, params)
            except Exception as e:
                logger.exception('(%s)consumer Got Exception:', _id)

    def multi_consume(self, _id):
        while self.continue_run:
            try:
                logger.debug('(%s)consumer waiting for msg', _id)
                msgs = self.c.consume(num_messages=100, timeout=3.0)
                if len(msgs) == 0: continue
                logger.debug('(%s)consumer got %s msgs', _id, len(msgs))
                params_str_seq = []
                for msg in msgs:
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                            logger.info('toppic(%s) [partition(%d)] Reached end at offset %d', 
                                        msg.topic(), msg.partition(), msg.offset())
                        else:
                            logger.exception('Kafka Error: %s', msg.error())
                        continue
                    params_str_seq.append(self.extract_info(msg))
                sql, querys = self.sqlbulider.build_many(params_str_seq)
                self.db.exe_many(sql, querys)
            except Exception as e:
                logger.exception('(%s)consumer Got Exception:', _id)

    def terminate(self):
        self.continue_run = False
    
    def close(self):
        self.c.close()
        self.db.close()   

    def test(self):
        try:
            while True:
                logger.debug('start fetching msgs')
                msgs = self.c.consume(num_messages=100, timeout=3.0)
                logger.debug('got %s msg', len(msgs))
                for msg in msgs:
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
                        sys.stderr.write('parition: %d, offset: %d, message: %s\n' %
                                            (msg.partition(), msg.offset(), msg.value()))
        except KeyboardInterrupt:
            sys.stderr.write('Aborted by user\n')

        self.c.close()
        

def run(cfg_path):
    cfg = ConfigHandler(cfg_path)
    consumer_num = cfg.kafka_info.partition_num
    consumers = [ Consumer(cfg) for i in range(consumer_num) ]

    try:
        thread_list = []
        for i, c in enumerate(consumers, 1):
            thread_list.append(threading.Thread(target=c.multi_consume, args=(i, )))

        map(lambda t: t.start(), thread_list)
        map(lambda t: t.join(), thread_list)
        logger.info('never reach the end of join')
        # add a new thread to listen kill signal
    except KeyboardInterrupt:
        logger.info('this will never happen!')
        map(lambda c: c.terminate(), consumers)
        raise

if __name__ == '__main__':

    cfg_path = 'consumer.yaml'

    # cfg = ConfigHandler(cfg_path)
    # c = Consumer(cfg)
    # c.multi_consume(1)

    run(cfg_path)
