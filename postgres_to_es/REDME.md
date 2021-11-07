### Как запускать

В коревой паке:
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cd postgres_to_es
python3 updater.py
```
В папке settings файл **config.toml**
требуется указать ip-адреса(или алиасы) ваших контейнеров postgres и elasticsearch 
(для удобства можно запустить docker-compose для elastic корневой папке)

Процесс можно запускать вне зависимости от того, запущены ли сейчас PG и ES, 
после остановки и перезапуска любого из них, процесс обновления индекса ES восстанавливается с момента прерывания соединений. (в папке misc есть тесты для Postman (ETLtests.json) для итогового результата)

Докер контейнер (подготовка к интеграции).
1) должны быть свободны порты 9200 и 5432 на хост машине (не заняты другими контейнерами)
```
docker build  -t etl:latest .
docker run -d --name etl -p "9200:9200" -p "5432:5432" etl:latest
docker logs -f etl
```
