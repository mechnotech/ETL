import logging
from functools import wraps
import time
from config import app_config

log = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


def backoff(
        start_sleep_time=app_config.backoff_start_sleep_time,
        factor=app_config.backoff_factor,
        border_sleep_time=app_config.backoff_border_sleep_time
):
    """
        Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
         Использует наивный экспоненциальный рост времени повтора (factor)
            до граничного времени ожидания (border_sleep_time)

        Формула:
            t = start_sleep_time * 2^(n) if t < border_sleep_time
            t = border_sleep_time if t >= border_sleep_time
        :param start_sleep_time: начальное время повтора
        :param factor: во сколько раз нужно увеличить время ожидания
        :param border_sleep_time: граничное время ожидания
        :return: результат выполнения функции
    """

    def my_decorator(f):

        @wraps(f)
        def wrapper(*args, **kwargs):
            t = start_sleep_time
            counter = 1
            while True:
                try:
                    time.sleep(t)
                    result = f(*args, **kwargs)
                    break

                except Exception:
                    log.exception(f'Connection failed, reconnection attempt #{counter}, wait time - {t} sec')
                    if t < border_sleep_time:
                        t = t * 2 ** factor
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    counter += 1

            return result

        return wrapper

    return my_decorator


if __name__ == '__main__':
    pass
