import json
import logging
import time
import datetime
from datetime import datetime as dt
import pytz
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

tables = ['fw', 'person', 'genre']


class ESConnector:
    # @backoff()
    def __init__(self):
        self.connection = Elasticsearch(host=es_config.ES_URL)
        self.connection.cluster.health(wait_for_status='yellow', request_timeout=1)
        self.block = []
        self.last_time = None
        self.state = State(JsonFileStorage())

    # @backoff()
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
            self.state.set_state('fw', self.last_time)

    def add_to_block(self, doc: dict, uuid: str):
        index_row = {"index": {"_index": "movies", "_id": f"{uuid}"}}
        self.block.append(json.dumps(index_row) + '\n' + json.dumps(doc))

    @property
    # @backoff()
    def is_index_exist(self):
        return self.connection.indices.exists(index=movies_index)

    @property
    # @backoff()
    def create_index(self):
        with open(es_config.default_scheme_file, 'r') as f:
            self.connection.indices.create(index=movies_index, body=json.load(f))
        return self.connection.indices.get(index=movies_index)

    def __del__(self):
        self.connection.close()


class PGConnector:

    # @backoff()
    def __init__(self):
        self.connection = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        self.cursor = self.connection.cursor()
        self.state = State(JsonFileStorage())
        self.last_time = None
        self.ids_to_update = []
        self.rows = None
        self.not_complete = {'fw': True, 'p': True, 'g': True}

    # @backoff()
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
        if not self.rows:
            return
        last_time = self.rows[-1][1]
        person_ids = "', '".join([x[0] for x in self.rows])
        self.get_data(f'''SELECT fw.id, fw.updated_at
                        FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    WHERE pfw.person_id IN ('{person_ids}')
                    ORDER BY fw.updated_at
                    LIMIT 1000;''')
        self.ids_to_update = self.rows
        self.state.set_state('p', last_time)

    def _push_genres(self):
        """Выбрать UUID фильмов затронутых изменением genres и поместить их в стек обработки"""
        if not self.rows:
            return
        last_time = self.rows[-1][1]
        genres_ids = "', '".join([x[0] for x in self.rows])
        self.get_data(f'''SELECT fw.id, fw.updated_at
                                FROM content.film_work fw
                            LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id
                            WHERE pfw.genre_id IN ('{genres_ids}')
                            ORDER BY fw.updated_at
                            LIMIT 1000;''')
        self.ids_to_update = self.rows
        self.state.set_state('g', last_time)

    def check_persons_updates(self):
        """Получение списка uuid и updated_at из таблицы person если были изменения
        """
        persons_last_time = self.state.get_state('p')
        self.get_data(
            f"SELECT id, updated_at FROM content.person WHERE updated_at > '{persons_last_time}'"
            f' order by updated_at DESC'
        )
        if len(self.rows) != 0:
            self._push_persons()
        self.not_complete['p'] = len(self.rows) != 0

    def check_genres_updates(self):
        """Получение списка uuid и updated_at из таблицы genre если были изменения
        """
        genres_last_time = self.state.get_state('g')
        self.get_data(
            f"SELECT id, updated_at FROM content.genre WHERE updated_at > '{genres_last_time}'"
            f' order by updated_at DESC'
        )
        if len(self.rows) != 0:
            self._push_genres()
        self.not_complete['g'] = len(self.rows) != 0

    def __del__(self):
        self.connection.close()


def updater(pg, es):
    for data in pg.next_to_update():
        es.add_to_block(*transformer(data))
        if len(es.block) >= es_config.bulk_factor:
            es.last_time = pg.last_time
            es.load()
    es.last_time = pg.last_time
    es.load()


def never_ending_process():
    es = ESConnector()
    pg = PGConnector()

    if not es.is_index_exist:
        result = es.create_index
        log.info(f'created index: {result}')

    while True:
        while pg.is_not_complete():
            pg.get_films_ids()
            updater(pg=pg, es=es)
            pg.check_persons_updates()
            updater(pg=pg, es=es)
            pg.check_genres_updates()
            updater(pg=pg, es=es)

        log.info(f'Nothing to update, now wait for {app_config.await_time} sec ...')
        time.sleep(app_config.await_time)
        pg.not_complete = {'fw': True, 'p': True, 'g': True}


if __name__ == '__main__':
    never_ending_process()
