import datetime
import json
import time
from datetime import datetime as dt

from config import es_config, app_config, genre_index, person_index
from logger import log
from backoff_decorator import backoff
from connectors import ESConnector, PGConnector
from postgres_to_es.make_fixtures import persons_fixtures, genres_fixtures
from storage import State, JsonFileStorage
from transform import genres_transformer, person_transformer

log = log.getChild(__name__)


class SideESConnector(ESConnector):
    def __init__(self):
        super().__init__()
        self.genres_to_es = []
        self.state = State(JsonFileStorage(file_path='misc/side.json'))
        self.block = []
        self.last_time = None

    @backoff()
    def upload(self, index):

        if not self.block:
            return
        body = '\n'.join(self.block)
        if index == person_index:
            persons_fixtures(self.block)
        if index == genre_index:
            genres_fixtures(self.block)
        res = self.connection.bulk(body=body, index=index, params={'filter_path': 'items.*.error'})
        if not res:
            log.info(f'Add block to index:{index} of {len(self.block)} records')
            self.block.clear()
            self.state.set_state(index, self.last_time)
        else:
            print(res)
            exit()

    def add_to_block_genres(self, doc: dict, uuid: str):
        index_row = {"index": {"_index": genre_index, "_id": f"{uuid}"}}
        self.block.append(json.dumps(index_row) + '\n' + json.dumps(doc))

    def add_to_block_person(self, doc: dict, uuid: str):
        index_row = {"index": {"_index": person_index, "_id": f"{uuid}"}}
        self.block.append(json.dumps(index_row) + '\n' + json.dumps(doc))


class SidePGConnector(PGConnector):
    def __init__(self):
        super().__init__()
        self.state = State(JsonFileStorage(file_path='misc/side.json'))
        self.new_genres = []
        self.new_persons = []
        self.last_time_genre = None
        self.last_time_person = None

    def set_start_time(self):
        """
        Если модуль запущен в первый раз:
        1) Устанавливает теоретическую дату начала времен кинотеатра для таблиц genres и person
        """
        self.last_time_genre = dt.fromisoformat('1999-01-01 12:00:00.000001+00:00')
        self.state.set_state(genre_index, self.last_time_genre)
        self.last_time_person = dt.fromisoformat('1999-01-01 12:00:00.000001+00:00')
        self.state.set_state(person_index, self.last_time_person)

    def check_persons_updates(self):

        if not self.state.get_state(person_index):
            self.set_start_time()
        self.last_time = dt.fromisoformat(self.state.get_state(person_index))
        person_last_time = self.state.get_state(person_index)
        data = '''SELECT id, full_name, birth_date, updated_at 
        FROM content.person 
        WHERE updated_at > %s 
        order by updated_at'''

        self.get_data(data, params=(person_last_time,))
        self.new_persons = self.rows

    def check_genres_updates(self):

        if not self.state.get_state(genre_index):
            self.set_start_time()
        self.last_time = dt.fromisoformat(self.state.get_state(genre_index))
        genres_last_time = self.state.get_state(genre_index)
        data = '''SELECT id, name, description, updated_at 
        FROM content.genre 
        WHERE updated_at > %s 
        order by updated_at'''

        self.get_data(data, params=(genres_last_time,))
        self.new_genres = self.rows

    def pop_next_to_update_genre(self):

        while self.new_genres:
            ids, name, description, updated_at = self.new_genres.pop(0)
            self.last_time_genre = updated_at
            yield ids, name, description, updated_at

    def pop_next_to_update_person(self):
        while self.new_persons:
            ids, full_name, birth_date, updated_at = self.new_persons.pop(0)
            if isinstance(birth_date, datetime.date):
                birth_date = json.dumps(birth_date, indent=4, sort_keys=True, default=str)[1:-1]
            self.last_time_person = updated_at
            yield ids, full_name, birth_date, updated_at


def genre_updater(pg, es):
    for data in pg.pop_next_to_update_genre():
        es.add_to_block_genres(*genres_transformer(data))
        if len(es.block) >= es_config.bulk_factor:
            es.last_time = pg.last_time_genre
            es.upload(genre_index)
    es.last_time = pg.last_time_genre
    es.upload(genre_index)


def person_updater(pg, es):
    for data in pg.pop_next_to_update_person():
        es.add_to_block_person(*person_transformer(data))
        if len(es.block) >= es_config.bulk_factor:
            es.last_time = pg.last_time_person
            es.upload(person_index)
    es.last_time = pg.last_time_person
    es.upload(person_index)


def side_check():
    es = SideESConnector()
    pg = SidePGConnector()

    pg.check_genres_updates()
    genre_updater(pg=pg, es=es)
    pg.check_persons_updates()
    person_updater(pg=pg, es=es)

    log.info(f'Persons and Genres moved to ES, no new data yet...')
