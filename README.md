Проект для компании CSBI

Решаемые задачи:

Ежедневное чтение предоставляемых по FTP списков (чтение версий файлов за заданную дату)
- парков
- водителей
- маршрутных заданий водителей при многосменной работе на маршруте.

Реализовано как регулярное по шедулеру и внеочередное по запросу (сконфигурировано посредством trigger beans in vis-import-config.xml).

Асинхронное параллельное (concurrent) чтение данных по всем паркам (19 шт.) реализовано при помощи 
предоставляемого Spring механизма (Async)TaskExecutor - TransactionTemplate.

В результате формируются путевые листы водителей с сохранением в БД.
Работа с БД Oracle происходит с использование Persistence entityManager API (для create, update, delete) и Criteria API (read)

***

The project for the CSBI, St Petersburg, former employer

Implemented tasks:

Daily reading of the files provided over FTP (finding and parsing of the files for the requested date)
- parks
- drivers
- driver route jobs in multi-shift operation

Implemented in two versions: run regularly by the scheduler and on-demand ((check trigger beans in vis-import-config.xml)).

Asynchronous parallel (concurrent) reading from all the park files (19 pcs.) implemented by means of Spring provided mechanism (Async)TaskExecutor - TransactionTemplate.

The driver waybills are formed and written into the database.
Interaction with Oracle database implemented by use of Persistence entityManager API (for create, update, delete operations) and the Criteria API (for read operations)
