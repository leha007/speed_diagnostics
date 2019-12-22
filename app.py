import configparser
import datetime
import logging
import os
from logging.handlers import RotatingFileHandler
from timeit import default_timer as timer

import pymongo
import speedtest

logger = logging.getLogger(__name__)
config = configparser.ConfigParser()


def init_logger(path):
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = RotatingFileHandler(path, maxBytes=5242880, backupCount=5, encoding='utf-8')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_speed_test_data():
    logger.debug('About to execute speed test')
    start = timer()

    threads = 2
    s = speedtest.Speedtest()
    s.get_best_server()
    s.download(threads=threads)
    s.upload(threads=threads)

    data = s.results.dict()
    logger.info(
        'Test result: Download: [{down:.2f}], Upload: [{up:.2f}]'.format(down=data['download'], up=data['upload']))

    return data, timer() - start


def save_to_mongo_db(test_data, exec_time, curr_time):
    logger.debug('Saving data to MongoDB')
    client = pymongo.MongoClient(config['MONGO']['DSN'])
    results_collection = client.services.speed_results

    data = {
        'test_time': curr_time,
        'execution_time': exec_time,
        'data': test_data
    }

    res = results_collection.insert_one(data)
    if res.acknowledged:
        logger.info('Data saved to MongoDB')
    else:
        logger.warning('Failed to save data to MongoDB')


def load_configuration(work_dir):
    prod_conf = os.path.join(work_dir, 'prod.conf')
    if os.path.isfile(prod_conf):
        logger.warning('Using production configuration file')
        config.read(prod_conf)
    else:
        default_conf = os.path.join(work_dir, 'default.conf')
        if os.path.isfile(default_conf):
            logger.warning('Using default configuration file')
            config.read(default_conf)
        else:
            raise FileNotFoundError('Default configuration file %s not found...' % default_conf)


def main():
    work_dir = os.path.dirname(os.path.realpath(__file__))
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    init_logger(os.path.join(work_dir, '{name}.log'.format(name=script_name)))

    logger.info('----Starting speed test----')

    try:
        load_configuration(work_dir)
        curr_time = datetime.datetime.now()
        data, exec_time = get_speed_test_data()
        save_to_mongo_db(data, exec_time, curr_time)
    except Exception:
        logger.error('General error', exc_info=True)

    logger.info('====Finished speed test====')


if __name__ == '__main__':
    main()
