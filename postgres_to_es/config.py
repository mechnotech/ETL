import logging
import toml

from postgres_to_es.data_classes import AppSettings, ESSettings, PGSettings

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

with open('settings/config.toml', 'r') as f:
    config = toml.load(f)
    pg_config = PGSettings.parse_obj(config['postgres'])
    es_config = ESSettings.parse_obj(config['elasticsearch'])
    app_config = AppSettings.parse_obj(config['app'])
    movies_index = es_config.movies_index
    dsl = {
        'dbname': pg_config.DB_NAME,
        'user': pg_config.POSTGRES_USER,
        'password': pg_config.POSTGRES_PASSWORD,
        'host': pg_config.DB_HOST,
        'port': pg_config.DB_PORT,
        'options': pg_config.options
    }
