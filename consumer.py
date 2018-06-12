#!/usr/bin/env python
# -*- coding:utf-8 -*-


import os
import sys
import yaml
import copy
import json
import decimal
import threading
import collections
import logging.config
import dateutil.parser
import confluent_kafka
import mysql.connector as DB
from datetime import datetime
from functools import partial


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


class SpecJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return dict(val=obj.isoformat(), _spec_type='datetime')
        elif isinstance(obj, decimal.Decimal):
            return dict(val=str(obj), _spec_type='decimal')
        else:
            return super(SpecJSONEncoder, self).default(obj)


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

# parse datetime by json
json.dumps = partial(json.dumps, cls=SpecJSONEncoder)
json.loads = partial(json.loads, object_hook=object_hook)

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
        except Exception as e:
            logger.exception('DB exception:')
        
    def exe(self, sql):
        try:
            self.cursor.execute(sql)
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

    params = ('instance_id', 'bot_id', 'bot_model_version', 
              'bot_code_version', 'bot_status', 'vm_ip', 'create_time', 'update_time', 'desc')

    QueryParams = collections.namedtuple('QueryParams', params)

    query = ("insert into t_unibot_status "
             "(instance_id, bot_id, bot_model_version, bot_code_version, bot_status, vm_ip, create_time, update_time, description) values "
             "({instance_id}, {bot_id}, {bot_model_version}, {bot_code_version}, '{bot_status}', '{vm_ip}', '{create_time}', '{update_time}', '{description}')")


    def __init__(self, params_info):
        self.params_info = params_info
        self.ime_format = '%Y-%m-%d %H:%M:%S.%f'

    def build(self, params):
        """
        create table t_unibot_status(id int(12) not null primary key auto_increment, instance_id int(12) not null, bot_id int(12) not null, 
        bot_model_version int(12) not null, bot_status varchar(32) not null, cmd_time timestamp(6) not null default CURRENT_TIMESTAMP(6), 
        exe_time timestamp(6) not null default CURRENT_TIMESTAMP(6) on update CURRENT_TIMESTAMP(6), description varchar(100)); 
        
        t_unibot_status
        (id, )instance_id, bot_id, bot_model_version, bot_status, create_time, update_time, description
        """
        
        checked_ok, params = self.check_and_fix(params)
        logger.debug('pass checked, %s', str(checked_ok))
        if not checked_ok:
            return None

        sql = self.query.format(**params)
        logger.info('Build sql: %s' % sql)
        
        return sql

    def dumps(self, _dict):
        def _converter(o):
            if isinstance(o, datetime):
                return o.strftime(self.time_format)
        return partial(json.dumps, default=_converter)(_dict)

    def check_and_fix(self, params):
        """
        TODO: use a better way!
        {"bot_status":"loading","instance_id":1137}
        {"bot_id":2148,"bot_model_version":3,"bot_status":"running","instance_id":1137}
        """
        _params = json.dumps
        # _params = self.QueryParams._make(params.split(','))._asdict()
        self.ori_params = copy.deepcopy(_params)

        logger.debug('Before checking and fixing params: \n==> %s', self.dumps(_params))

        if not _params.get('instance_id'):
            self.check_failed('instance_id')
            return False, self.ori_params

        if not _params.get('bot_status'):
            self.check_failed('bot_status')
            return False, self.ori_params

        if not _params.get('bot_id'):
            _params['bot_id'] = 0

        if not _params.get('bot_model_version'):
            _params['bot_model_version'] = 3

        if not _params.get('bot_code_version'):
            _params['bot_code_version'] = 3
        
        if not _params.get('create_time'):
            _params['create_time'] = datetime.now().strftime(self.time_format)
        else:
            _params['create_time'] = datetime.strptime(_params['create_time'], self.time_format)

        if not _params.get('update_time'):
            _params['update_time'] = datetime.now().strftime(self.time_format)
        else:
            _params['update_time'] = datetime.strptime(_params['update_time'], self.time_format)

        if not _params.get('vm_ip'):
            _params['vm_ip'] = ''

        if not _params.get('desc'):
            _params['desc'] = 'no need desc!'

        logger.debug('After fixing params: \n==> %s', self.dumps(_params))
        return True, _params

    def check_and_fix_params(self, params_str):
        logger.debug('Before checking and fixing params: \n==> %s', params_str)

        params = json.loads(params_str)
        for param, info in self.params_info.items():
            logger.debug('prams: %s, info: %s', param, info)
            if not params.get(param) and info.get('required'):
                logger.warn('Params illegal, %s is required!', param)
                return None
            elif not params.get(param):
                value = eval(info.get('default'))
                logger.debug('*****%s, type: %s', value, type(value))
                params[param] = str(value)
        logger.debug('Params checked and fixed:\n ==> %s', json.dumps(params))
        return params
                
    def build_params(self, params_str):
        """
        create table t_unibot_status(id int(12) not null primary key auto_increment, instance_id int(12) not null, bot_id int(12) not null, 
        bot_model_version int(12) not null, bot_status varchar(32) not null, cmd_time timestamp(6) not null default CURRENT_TIMESTAMP(6), 
        exe_time timestamp(6) not null default CURRENT_TIMESTAMP(6) on update CURRENT_TIMESTAMP(6), description varchar(100)); 
        
        t_unibot_status
        (id, )instance_id, bot_id, bot_model_version, bot_status, create_time, update_time, description
        """
        
        params = self.check_and_fix_params(params_str)
        if not params:
            return None

        sql = self.query.format(**params)
        logger.info('Build sql: %s' % sql)
        
        return sql

    def build_many(self, params_str_seq):
        params_seq = []
        for params_str in params_str_seq:
            param = self.build_params(params_str)
            if param is None:
                continue 
            params_seq.append(param)
        return self.query, params_seq

class Consumer(object):
    def __init__(self, conf):
        self.conf = conf
        self.init_kafka()
        self.sqlbulider = SqlBuilder(cfg.query_params)
        self.db = DBHandler(conf.mysql_info)

        self.continue_run = True

    def init_kafka(self):
        conn_info = self.conf.kafka_info.conn
        self.c = confluent_kafka.Consumer(**conn_info)

        topics = self.conf.kafka_info.topics
        self.c.subscribe(topics)


    def run(self, _id):
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
                    value = msg.value()
                    query = self.sqlbulider.build_params(value)
                    if query is not None:
                        self.db.exe(query)
                        logger.debug('%s exe sql', _id)
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
            thread_list.append(threading.Thread(target=c.run, args=(i, )))

        map(lambda t: t.start(), thread_list)
        map(lambda t: t.join(), thread_list)
        logger.info('never reach the end of join')
        # add a new thread to listen kill signal
    except KeyboardInterrupt:
        logger.info('this will never happen!')
        map(lambda c: c.terminate(), consumers)
        raise


def test_logging():
    setup_logging()
    logger = logging.getLogger('dev')
    logger.info('Hello INFO!')

def test_json():
    data = {
        "hello": "world",
        "thing": datetime.now(),
        "other": decimal.Decimal(0)
    }

    thing = json.dumps(data)
    data = json.loads(thing)
    return thing, data


if __name__ == '__main__':

    cfg_path = 'consumer.yaml'

    cfg = ConfigHandler(cfg_path)
    c = Consumer(cfg)

    c.run(1)

    # run(cfg_path)


'''
todos:
-1 parse json_str to dict (datetime)
-2 test mysql.connector executemany met
-3 check_and_fix params by a better way
'''