from dataclasses import dataclass

from pydantic import BaseModel


class AppSettings(BaseModel):
    backoff_start_sleep_time: float
    backoff_factor: int
    backoff_border_sleep_time: int
    await_time: float
    storage_file_path: str


class ESSettings(BaseModel):
    ES_URL: str
    ES_PORT: int
    movies_index: str
    bulk_factor: int
    default_scheme_file: str


class PGSettings(BaseModel):
    DB_NAME: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    DB_HOST: str
    DB_PORT: int
    options: str
    bulk_factor: int
    sql_get_oldest_time: str
    sql_get_new_ids: str
    sql_get_film: str


@dataclass
class MovieRaw:
    __slots__ = (
        'uuid', 'title', 'description', 'imdb_rating', 'role', 'person_id', 'person_name', 'genre',
    )
    uuid: str
    title: str
    description: str
    imdb_rating: float
    role: str
    person_id: str
    person_name: str
    genre: str
