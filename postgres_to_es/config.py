import toml

from data_classes import AppSettings, ESSettings, PGSettings

with open('settings/config.toml', 'r') as f:
    config = toml.load(f)
    pg_config = PGSettings.parse_obj(config['postgres'])
    es_config = ESSettings.parse_obj(config['elasticsearch'])
    app_config = AppSettings.parse_obj(config['app'])
    movies_index = es_config.movies_index
    genre_index = es_config.genre_index
    person_index = es_config.person_index
    dsl = {
        'dbname': pg_config.DB_NAME,
        'user': pg_config.POSTGRES_USER,
        'password': pg_config.POSTGRES_PASSWORD,
        'host': pg_config.DB_HOST,
        'port': pg_config.DB_PORT,
        'options': pg_config.options
    }
