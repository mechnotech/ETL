import json
import time

from config import es_config, app_config, movies_index, genre_index
from logger import log
from postgres_to_es.updater import ESConnector, PGConnector
from transform import genres_transformer
from storage import State, JsonFileStorage

log = log.getChild(__name__)


class SideESConnector(ESConnector):
    def __init__(self):
        super().__init__()
        self.genres_to_es = []
        self.state = State(JsonFileStorage(file_path='misc/side.json'))

    def load(self, index):

        if not self.block:
            return
        body = '\n'.join(self.block)
        res = self.connection.bulk(body=body, index=index, params={'filter_path': 'items.*.error'})
        if not res:
            log.info(f'Add block to index:{index} of {len(self.block)} records')
            self.block.clear()
            self.state.set_state(index, self.last_time)
        else:
            print(res)

    def add_to_block_genres(self, doc: dict, uuid: str):
        index_row = {"index": {"_index": 'genres', "_id": f"{uuid}"}}
        self.block.append(json.dumps(index_row) + '\n' + json.dumps(doc))


class SidePGConnector(PGConnector):
    def __init__(self):
        super().__init__()
        self.state = State(JsonFileStorage(file_path='misc/side.json'))



    # def check_persons_updates(self):
    #     """Получение списка uuid и updated_at из таблицы person если были изменения
    #     """
    #     persons_last_time = self.state.get_state(_person_index')
    #     data =
    #     self.get_data(pg_config.sql_check_persons, params=(persons_last_time,))
    #     if len(self.rows) != 0:
    #         self._push_persons()
    #     self.not_complete['p'] = len(self.rows) != 0
    #
    def check_genres_updates(self):
        """Получение списка uuid и updated_at из таблицы genre если были изменения
        """
        genres_last_time = self.state.get_state(genre_index)
        data = '''SELECT id, name, description 
        FROM content.genre 
        WHERE updated_at > %s 
        order by updated_at DESC'''

        self.get_data(data, params=(genres_last_time,))

        if len(self.rows) != 0:
            self._push_genres()


def updater(pg, es):
    for data in pg.pop_next_to_update():
        es.add_to_block_genres(*genres_transformer(data))
        if len(es.block) >= es_config.bulk_factor:
            es.last_time = pg.last_time
            es.load(genre_index)
    es.last_time = pg.last_time
    es.load()


def never_ending_process():
    es = SideESConnector()
    pg = SidePGConnector()

    while True:

        pg.check_genres_updates()
        updater(pg=pg, es=es)
            # pg.check_persons_updates()
            # updater(pg=pg, es=es)


        log.info(f'Nothing to update, now wait for {app_config.await_time} sec ...')
        time.sleep(app_config.await_time)
        pg.not_complete = {'p': True, 'g': True}

if __name__ == '__main__':
    never_ending_process()