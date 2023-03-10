# Проект 7-го спринта

### Описание задачи
В продукт планируют внедрить систему рекомендации друзей. Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:
- состоят в одном канале,
- раньше никогда не переписывались,
- находятся не дальше 1 км друг от друга.

При этом команда хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:
Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.

Определить, как часто пользователи путешествуют и какие города выбирают.
Благодаря такой аналитике в соцсеть можно будет вставить рекламу: приложение сможет учитывать местонахождение пользователя и предлагать тому подходящие услуги компаний-партнёров. 

В проекте используются почти те же данные, что и в спринте. Есть только два изменения:

В таблицу событий добавились два поля — широта и долгота исходящих сообщений. Обновлённая таблица уже находится в HDFS по этому пути: /user/master/data/geo/events.
У вас теперь есть координаты городов Австралии, которые аналитики собрали в одну таблицу — geo.csv. Скачайте её и добавьте в HDFS. Для этого загрузите таблицу в Jupyter, воспользовавшись командой copyFromLocal в терминале.

### Основная цель:
- [x] 1. Обновить структуру Data Lake
- [x] 2. Создать витрину в разрезе пользователей
- [x] 3. Создать витрину в разрезе зон
- [x] 4. Построить витрину для рекомендации друзей
- [x] 5. Автоматизировать обновление витрин

### 1) Создать витрину в разрезе пользователей
Определите, в каком городе было совершено событие, выясните, сколько пользователь путешествует

Необходимы поля
user_id — идентификатор пользователя.
act_city — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
home_city — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
travel_count — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
travel_array — список городов в порядке посещения.
TIME_UTC — время в таблице событий. Указано в UTC+0.
timezone — актуальный адрес. Атрибуты содержатся в виде Australia/Sydney.

hdfs:/user/imrtnv/marts/users

### 2) Создать витрину в разрезе зон
Нужно посчитать количество событий в конкретном городе за неделю и месяц. витрина будет содержать следующие поля:

month — месяц расчёта;
week — неделя расчёта;
zone_id — идентификатор зоны (города);
week_message — количество сообщений за неделю;
week_reaction — количество реакций за неделю;
week_subscription — количество подписок за неделю;
week_user — количество регистраций за неделю;
month_message — количество сообщений за месяц;
month_reaction — количество реакций за месяц;
month_subscription — количество подписок за месяц;
month_user — количество регистраций за месяц.

В этой витрине вы учитываете не только отправленные сообщения, но и другие действия — подписки, реакции, регистрации (рассчитываются по первым сообщениям). Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя.

hdfs:/user/imrtnv/marts/geo

### 3) Построить витрину для рекомендации друзей
Напомним, как будет работать рекомендация друзей: если пользователи подписаны на один канал, ранее никогда не переписывались и расстояние между ними не превышает 1 км, то им обоим будет предложено добавить другого в друзья. Образовывается парный атрибут, который обязан быть уникальным: порядок упоминания не должен создавать дубли пар.

Витрина будет содержать следующие атрибуты:
user_left — первый пользователь;
user_right — второй пользователь;
processed_dttm — дата расчёта витрины;
zone_id — идентификатор зоны (города);
local_time — локальное время.

dfs:/user/imrtnv/marts/friend_recomendation

### Слои хранилища
RAW - /user/master/data/geo/events/ - данные по событиям (hdfs)
STG - /user/imrtnv/data/events/ (hdfs)
DDS - geo - /user/imrtnv/data/citygeodata/geo.csv (hdfs)
DDS - events - /user/imrtnv/data/events/date=yyyy-mm-dd (hdfs)
Mart - /user/imrtnv/data/marts (hdfs)

geo - разрезе зон
users - в разрезе пользователей


### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags` - DAG - файлы
	- `DAG_initial_load.py` — Инициальная партиционорование данных  выполняется в ручную (однакратно)  
	- `DAG_main_calc_marts.py` — DAG обновляет слой STG из источника и произвоодит расчет метирик ветрины (на заданную глубину).
	
- `/src/sqripts` - py файлы c job'ами
	- `initial_load_job.py` — Job инициальной загрузки.
	- `calculating_user_analitics_job.py` — Job расчета пользовательских метрик и сохранения витрины.
	- `calculating_geo_analitics_job.py` — Job расчета geo метрик и сохранения витрины.
	- `calculating_friend_recomendation_analitics_job.py` - Job расчета метрик ветрины рекомендации друзей.
	- `update_stg_by_date_job.py`  — Job обновления STG.
