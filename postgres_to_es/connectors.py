import json
from datetime import datetime as dt

import psycopg2
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from backoff_decorator import backoff
from config import pg_config, es_config, dsl, movies_index
from logger import log
from make_fixtures import movies_fixtures
from storage import State, JsonFileStorage


class ESConnector:
    @backoff(logy=log.getChild('ESConnector.init'))
    def __init__(self):
        self.connection = Elasticsearch(host=es_config.ES_URL)
        self.connection.cluster.health(wait_for_status='yellow', request_timeout=1)
        self.block = []
        self.last_time = None
        self.state = State(JsonFileStorage())

    @backoff(logy=log.getChild('ESConnector.load'))
    def load(self):
        """ Загрузить в ES пачку подготовленных данных и записать в состояния
        время последней строки
        """

        if not self.block:
            return
        body = '\n'.join(self.block)
        movies_fixtures(self.block)
        res = self.connection.bulk(body=body, index=movies_index, params={'filter_path': 'items.*.error'})
        if not res:
            log.info(f'Add block of {len(self.block)} records')
            self.block.clear()
            self.state.set_state('fw', self.last_time)
        else:
            print(res)

    def add_to_block(self, doc: dict, uuid: str):
        index_row = {"index": {"_index": "movies", "_id": f"{uuid}"}}
        self.block.append(json.dumps(index_row) + '\n' + json.dumps(doc))

    @backoff(logy=log.getChild('ESConnector.is_index_exist'))
    def is_index_exist(self, index: str):
        return self.connection.indices.exists(index=index)

    @backoff(logy=log.getChild('ESConnector.create_index'))
    def create_indexes(self, index: str, file_name: str):

        with open(file_name, 'r') as f:
            self.connection.indices.create(index=index, body=json.load(f))
            return self.connection.indices.get(index=index)

    def __del__(self):
        self.connection.close()


class PGConnector:

    @backoff(logy=log.getChild('PGConnector.init'))
    def __init__(self):
        self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.cursor = self.connection.cursor()
        self.state = State(JsonFileStorage())
        self.last_time = None
        self.rows = None
        self.genres_to_update = []
        self.not_complete = {'fw': True, 'p': True, 'g': True}

    @backoff(logy=log.getChild('PGConnector.get_data'))
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

    def set_start_time(self):
        """
        Если модуль запущен в первый раз:
        1) Устанавливает топ времени для таблиц p - person и g - genre
        2) Устанавливает теоретическую дату начала времен кинотеатра для таблицы fw - film_works
        """
        self.get_data(pg_config.sql_get_top_time_person, size=1)
        p_time = self.rows[0][0]
        self.state.set_state('p', p_time)
        self.get_data(pg_config.sql_get_top_time_genre, size=1)
        g_time = self.rows[0][0]
        self.state.set_state('g', g_time)
        self.last_time = dt.fromisoformat('1999-01-01 12:00:00.000001+00:00')
        self.state.set_state('fw', self.last_time)

    def is_not_complete(self):
        for _, v in self.not_complete.items():
            if v:
                return True
        return False

    def get_films_ids(self):
        """Получение списка непроиндексированных записей фильмов (их uuid и updated_at)"""
        if not self.state.get_state('fw'):
            self.set_start_time()
        self.last_time = dt.fromisoformat(self.state.get_state('fw'))
        self.get_data(pg_config.sql_get_new_ids, params=[self.last_time, ], size=pg_config.bulk_factor)
        self.ids_to_update = self.rows
        self.not_complete['fw'] = len(self.rows) != 0

    def pop_next_to_update(self):
        """Получение списка денормизированных данных общей таблицы по фильмам для uuid из стека"""
        while self.ids_to_update:
            ids, updated_at = self.ids_to_update.pop(0)
            self.get_data(pg_config.sql_get_film, params=[ids, ])
            if self.last_time < updated_at:
                self.last_time = updated_at
            yield self.rows

    def _push_persons(self):
        """Выбрать UUID фильмов затронутых изменением person и поместить их в стек обработки"""

        last_time = self.rows[-1][1]
        person_ids = tuple([x[0] for x in self.rows])
        self.get_data(pg_config.sql_push_persons, params=(person_ids,))
        self.ids_to_update = self.rows
        self.state.set_state('p', last_time)

    def _push_genres(self):
        """Выбрать UUID фильмов затронутых изменением genres и поместить их в стек обработки"""

        last_time = self.rows[-1][1]
        genres_ids = tuple([x[0] for x in self.rows])
        self.get_data(pg_config.sql_push_genres, params=(genres_ids,))
        self.ids_to_update = self.rows
        self.state.set_state('g', last_time)

    def check_persons_updates(self):
        """Получение списка uuid и updated_at из таблицы person если были изменения
        """
        persons_last_time = self.state.get_state('p')
        self.get_data(pg_config.sql_check_persons, params=(persons_last_time,))
        if len(self.rows) != 0:
            self._push_persons()
        self.not_complete['p'] = len(self.rows) != 0

    def check_genres_updates(self):
        """Получение списка uuid и updated_at из таблицы genre если были изменения
        """
        genres_last_time = self.state.get_state('g')
        self.get_data(pg_config.sql_check_genres, params=(genres_last_time,))

        if len(self.rows) != 0:
            self._push_genres()
        self.not_complete['g'] = len(self.rows) != 0

    def __del__(self):
        self.connection.close()
