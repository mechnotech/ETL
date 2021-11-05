### Как запускать

В коревой паке:
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python3 postgres_to_es/updater.py
```
В папке settings файл **config.toml**
требуется указать ip-адреса(или алиасы) ваших контейнеров postgres и elasticsearch 
(для удобства можно запустить docker-compose для elastic в папке misc)

Процесс можно запускать вне зависимости от того, запущены ли сейчас PG и ES, 
после остановки и перезапуска любого из них, процесс обновления индекса ES восстанавливается с момента прерывания соединений. (в папке misc есть тесты для Postman (ETLtests.json) для итогового результата)

