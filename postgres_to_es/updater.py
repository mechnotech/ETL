import time

from config import es_config, app_config, movies_index, genre_index, person_index
from connectors import ESConnector, PGConnector
from logger import log
from side_updater import side_check
from transform import transformer

log = log.getChild(__name__)


def updater(pg, es):
    for data in pg.pop_next_to_update():
        es.add_to_block(*transformer(data))
        if len(es.block) >= es_config.bulk_factor:
            es.last_time = pg.last_time
            es.load()
    es.last_time = pg.last_time
    es.load()


def never_ending_process():
    es = ESConnector()
    pg = PGConnector()
    indexes_files = {
        movies_index: es_config.default_scheme_file,
        genre_index: es_config.genres_scheme_file,
        person_index: es_config.persons_scheme_file,
    }

    for index, file in indexes_files.items():
        if not es.is_index_exist(index):
            result = es.create_indexes(index, file)
            log.info(f'created index: {result}')

    while True:
        while pg.is_not_complete():
            pg.get_films_ids()
            updater(pg=pg, es=es)
            pg.check_persons_updates()
            updater(pg=pg, es=es)
            pg.check_genres_updates()
            updater(pg=pg, es=es)
            side_check()

        log.info(f'Nothing to update, now wait for {app_config.await_time} sec ...')
        time.sleep(app_config.await_time)
        pg.not_complete = {'fw': True, 'p': True, 'g': True}


if __name__ == '__main__':
    never_ending_process()
