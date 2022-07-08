import sqlalchemy


engine = sqlalchemy.create_engine('postgresql+psycopg2://:@localhost:5432/postgres')
connection = engine.connect()


code = """
SELECT g.name, COUNT(ga.author_id) FROM genre g
    JOIN genreauthor ga ON g.id = ga.genre_id
    GROUP BY g.id;
"""
res = connection.execute(code).fetchall()
print(f'1. Кол-во исполнителей по жанрам: {res}')


code = """
SELECT COUNT(t.id) FROM album a
    JOIN track t ON a.id = t.album_id
    WHERE a.year BETWEEN 2019 AND 2020
    GROUP BY t.id;
"""
res = len(connection.execute(code).fetchall())
print(f'2. Кол-во треков, вошедших в альбомы 2019-2020 гг.: {res}')


code = """
SELECT a.name, AVG(t.length) FROM album a
    JOIN track t ON a.id = t.album_id
    GROUP BY a.id;
"""
res = connection.execute(code).fetchall()
print(f'3. Средняя продолжительность треков по каждому альбому (сек.): {[(i[0], float(i[1])) for i in res]}')


code = """
SELECT DISTINCT au.name FROM album al
    JOIN albumauthor aa ON al.id = aa.album_id
    JOIN author au ON aa.author_id = au.id
    WHERE au.id NOT IN (SELECT DISTINCT au.id FROM album al
                            JOIN albumauthor aa ON al.id = aa.album_id
                            JOIN author au ON aa.author_id = au.id
                            WHERE al.year = 2020);
"""
res = connection.execute(code).fetchall()
print(f'4. Исполнители, не выпустившие альбомы в 2020 г.: {[i[0] for i in res]}')


code = """
SELECT DISTINCT c.name FROM collection c
    JOIN collectiontrack ct ON c.id = ct.collection_id
    JOIN track t ON ct.track_id = t.id
    JOIN album a ON t.album_id = a.id
    JOIN albumauthor aa ON a.id = aa.album_id
    JOIN author au ON aa.author_id = au.id
    WHERE au.name LIKE 'Louis Armstrong'
"""
res = connection.execute(code).fetchall()
print(f'5. Сборники, в которых присутствует исполнитель Louis Armstrong: {[i[0] for i in res]}')


code = """
SELECT a.name FROM album a
    JOIN albumauthor aa ON a.id = aa.album_id
    JOIN author au ON aa.author_id = au.id
    JOIN genreauthor ga ON au.id = ga.author_id
    GROUP BY a.id
    HAVING COUNT(ga.genre_id) > 1;
"""
res = connection.execute(code).fetchall()
print(f'6. Альбомы, в которых есть исполнители более 1-го жанра: {"NULL" if len(res) == 0 else [i[0] for i in res]}')


code = """
SELECT t.name FROM track t
    LEFT JOIN collectiontrack ct ON t.id = ct.track_id
    LEFT JOIN collection c ON ct.collection_id = c.id
    WHERE c.id IS NULL;
"""
res = connection.execute(code).fetchall()
print(f'7. Треки, которые не входят в сборники: {[i[0] for i in res]}')


code = """
SELECT DISTINCT au.name FROM track t
    JOIN album a ON t.album_id = a.id
    JOIN albumauthor aa ON a.id = aa.album_id
    JOIN author au ON aa.author_id = au.id
    WHERE t.length = (SELECT MIN(length) FROM track)
"""
res = connection.execute(code).fetchall()
print(f'8. Исполнители, написавшие самый короткий по продолжительности трек: {[i[0] for i in res]}')


code = """
SELECT a.name FROM album a
    JOIN track t ON a.id = t.album_id
    GROUP BY a.id
    HAVING COUNT(t.id) = (SELECT COUNT(t.id) FROM album a
                            JOIN track t ON a.id = t.album_id
                            GROUP BY a.id
                            ORDER BY COUNT(t.id)
                            LIMIT 1);
"""
res = connection.execute(code).fetchall()
print(f'9. Альбомы с наименьшим кол-вом треков: {[i[0] for i in res]}')
