import abc
import json
import logging
from abc import ABC
from typing import Any
from config import app_config

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


class BaseStorage:
    @abc.abstractmethod
    def check_or_create(self):
        try:
            with open(self.file_path, 'r') as f:
                pass
        except FileNotFoundError:
            log.exception(f'Now create file {self.file_path}')
            with open(self.file_path, 'w') as f:
                pass

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        self.check_or_create()
        with open(self.file_path, 'w') as f:
            f.write(json.dumps(state, indent=4, sort_keys=True, default=str))

    @abc.abstractmethod
    def retrieve_state(self):
        """Загрузить состояние локально из постоянного хранилища"""
        self.check_or_create()
        with open(self.file_path, 'r') as f:
            file_data = f.read()
            if not file_data:
                log.error(f'State file - {self.file_path} is empty')
                return {}
            try:
                return json.loads(file_data)
            except Exception:
                log.exception(f'Wrong state file format - {self.file_path}')
                return


class JsonFileStorage(BaseStorage, ABC):
    def __init__(self, file_path=app_config.storage_file_path):
        self.file_path = file_path


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage

    def set_state(self, value: Any, key='film_work') -> None:
        self.storage.save_state({key: value})

    def get_state(self, key='film_work') -> Any:
        res = self.storage.retrieve_state()
        return res.get(key)


if __name__ == '__main__':
    pass
