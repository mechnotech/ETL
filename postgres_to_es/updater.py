import json
import logging
import time

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from backoff_decorator import backoff
from config import pg_config, es_config, app_config, dsl, movies_index
from storage import State, JsonFileStorage
from transform import transformer

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


class ESConnector:
    @backoff()
    def __init__(self):
        self.connection = Elasticsearch(host=es_config.ES_URL)
        self.connection.cluster.health(wait_for_status='yellow', request_timeout=1)
        self.block = []
        self.last_time = None
        self.state = State(JsonFileStorage())

    @backoff()
    def load(self):
        """ Загрузить в ES пачку подготовленных данных и записать в состояния
        время последней строки
        """

        if not self.block:
            return
        body = '\n'.join(self.block)
        res = self.connection.bulk(body=body, index=movies_index, params={'filter_path': 'items.*.error'})
        if not res:
            log.info(f'Add block of {len(self.block)} records')
            self.block.clear()
            self.state.set_state(value=self.last_time)

    def add_to_block(self, doc: dict, uuid: str):
        index_row = {"index": {"_index": "movies", "_id": f"{uuid}"}}
        self.block.append(json.dumps(index_row) + '\n' + json.dumps(doc))

    @property
    @backoff()
    def is_index_exist(self):
        return self.connection.indices.exists(index=movies_index)

    @property
    @backoff()
    def create_index(self):
        with open(es_config.default_scheme_file, 'r') as f:
            self.connection.indices.create(index=movies_index, body=json.load(f))
        return self.connection.indices.get(index=movies_index)

    def __del__(self):
        self.connection.close()


class PGConnector:

    @backoff()
    def __init__(self):
        self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.cursor = self.connection.cursor()
        self.state = State(JsonFileStorage())
        self.last_time = self.state.get_state()
        self.ids_to_update = None
        self.rows = None

    @backoff()
    def get_data(self, execute: str, params=None, size=None):
        """ Получить данные из Postgres
         : execute SQL запрос
         : params Параметры SQL запроса если таковые имеются
         : size размер блока записей
        """
        if params is None:
            params = []
        if self.connection.close:
            self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
            self.cursor = self.connection.cursor()
        self.cursor.execute(execute, vars=params)
        if size:
            self.rows = self.cursor.fetchmany(size=size)
        else:
            self.rows = self.cursor.fetchall()

    def get_oldest_time(self):
        """
        Если модуль запущен в первый раз, получает самую старую запись и устанавливает
        состояние на неё
        """
        self.get_data(pg_config.sql_get_oldest_time, size=1)
        self.last_time = self.rows[0][0]
        self.state.set_state(value=self.last_time)

    def get_new_ids(self):
        """Получение списка непроиндексированных записей (их uuid и updated_at)"""
        if not self.state.get_state():
            self.get_oldest_time()
        self.get_data(pg_config.sql_get_new_ids, params=[self.last_time, ])
        self.ids_to_update = self.rows

    def next_to_update(self):
        """Получение списка денормизированных данных для uuid"""
        for row in self.ids_to_update:
            self.get_data(pg_config.sql_get_film, params=[row[0]])
            self.last_time = row[1]
            yield self.rows

    def __del__(self):
        self.connection.close()


def never_ending_process():
    es = ESConnector()
    pg = PGConnector()

    if not es.is_index_exist:
        result = es.create_index
        log.info(f'created index: {result}')

    while True:

        pg.get_new_ids()
        if len(pg.ids_to_update) != 1:
            for data in pg.next_to_update():
                es.add_to_block(*transformer(data))
                if len(es.block) >= es_config.bulk_factor:
                    es.last_time = pg.last_time
                    es.load()
            es.last_time = pg.last_time
            es.load()

        log.info(f'Nothing to update, now wait for {app_config.await_time} sec ...')
        time.sleep(app_config.await_time)


if __name__ == '__main__':
    never_ending_process()
