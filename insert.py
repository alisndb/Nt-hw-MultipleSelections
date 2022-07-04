import sqlalchemy


engine = sqlalchemy.create_engine('postgresql+psycopg2://:@localhost:5432/postgres')
connection = engine.connect()

library = {
    'Alternative': {
        'Portugal. The Man': [2019, {
            'Woodstock': [['So Young', 162], ['Feel It Still', 197]]
        }]
    },
    'Jazz': {
        'Frank Sinatra': [1966, {
            'Moonlight Sinatra': [['Oh, You Crazy Moon', 324], ['Moonlight Serenade', 293]]
        }],
        'Louis Armstrong': [1958, {
            'Louis Under the Stars': [['Stormy Weather', 322], ['My Home', 181]]
        }]
    },
    'Hip-hop': {
        'Coolio': [1997, {
            'My Soul': [['Can U Dig It', 224], ['Can I Get Down 1x', 145]]
        }],
        '2Pac': [1996, {
            'All Eyez On Me': [['My Life Goes On', 365], ['Check Out Time', 191]]
        }]
    },
    'Pop': {
        'Lorde': [2018, {
            'Melodrama': [['Green Light', 126], ['Liability', 149]]
        }],
        'Taylor Swift': [2014, {
            '1989': [['Shake It Off', 167], ['Clean', 204]]
        }]
    },
    'Electronic': {
        'Oliver': [2020, {
            'Full Circle': [['Ottomatic', 201], ['Last Forever', 237]]
        }]
    }
}

collections = [
    [['Col. 1', 2017], ['Ottomatic', 'Shake It Off', 'Clean']],
    [['Col. 2', 2018], ['Green Light', 'My Life Goes On', 'Ottomatic']],
    [['Col. 3', 2019], ['Oh, You Crazy Moon', 'My Home', 'So Young']],
    [['Col. 4', 2020], ['Moonlight Serenade', 'Last Forever', 'Can U Dig It']],
    [['Col. 5', 2020], ['Clean', 'Check Out Time', 'Stormy Weather']],
    [['Col. 6', 2021], ['Green Light', 'Shake It Off', 'Feel It Still']],
    [['Col. 7', 2022], ['Liability', 'Moonlight Serenade', 'Oh, You Crazy Moon']],
    [['Col. 8', 2022], ['Stormy Weather', 'Ottomatic', 'My Life Goes On']],
]


code = """
INSERT INTO genre(name)
    VALUES(%s);
"""
for var in library:
    data = (var, )
    connection.execute(code, data)


code = """
INSERT INTO author(name)
    VALUES(%s);
"""
for var in [library[i].keys() for i in library]:
    for aut in var:
        data = (aut, )
        connection.execute(code, data)


code = """
INSERT INTO collection(name, year)
    VALUES(%s, %s);
"""
for var in collections:
    data = (var[0][0], var[0][1], )
    connection.execute(code, data)


code = """
INSERT INTO album(name, year)
    VALUES(%s, %s);
"""
for var in [list(library[i].values()) for i in library]:
    for content in var:
        data = (list(content[1].keys())[0], content[0], )
        connection.execute(code, data)


code = """
INSERT INTO track(album_id, name, length)
    VALUES(%s, %s, %s);
"""
request = """
SELECT id FROM album
    WHERE name LIKE %s;
"""
for var in [list(library[i].values()) for i in library]:
    content = [var[1] for var in list(var)]
    for pairs in content:
        album = list(pairs.keys())[0]
        tracks = list(pairs.values())[0]
        res = connection.execute(request, album).fetchone()
        id_int = int(str(res)[1:].replace(',)', ''))
        for info in tracks:
            data = (id_int, info[0], info[1], )
            connection.execute(code, data)


code = """
INSERT INTO genreauthor(genre_id, author_id)
    VALUES(%s, %s);
"""
genre_req = """
SELECT id FROM genre
    WHERE name LIKE %s;
"""
author_req = """
SELECT id FROM author
    WHERE name LIKE %s;
"""
for gen, content in library.items():
    genre_res = connection.execute(genre_req, gen).fetchone()
    genre_id_int = int(str(genre_res)[1:].replace(',)', ''))
    for aut in content:
        author_res = connection.execute(author_req, aut).fetchone()
        author_id_int = int(str(author_res)[1:].replace(',)', ''))
        data = (genre_id_int, author_id_int, )
        connection.execute(code, data)


code = """
INSERT INTO albumauthor(album_id, author_id)
    VALUES(%s, %s);
"""
album_req = """
SELECT id FROM album
    WHERE name LIKE %s;
"""
author_req = """
SELECT id FROM author
    WHERE name LIKE %s;
"""
for var in [library[i].items() for i in library]:
    for content in var:
        aut = content[0]
        author_res = connection.execute(author_req, aut).fetchone()
        author_id_int = int(str(author_res)[1:].replace(',)', ''))
        alb = content[1][1].keys()
        for el in alb:
            album_res = connection.execute(album_req, el).fetchone()
            album_id_int = int(str(album_res)[1:].replace(',)', ''))
            data = (album_id_int, author_id_int, )
            connection.execute(code, data)


code = """
INSERT INTO collectiontrack(collection_id, track_id)
    VALUES(%s, %s);
"""
collection_req = """
SELECT id FROM collection
    WHERE name LIKE %s;
"""
track_req = """
SELECT id FROM track
    WHERE name LIKE %s;
"""
for var in collections:
    collection_res = connection.execute(collection_req, var[0][0]).fetchone()
    collection_id_int = int(str(collection_res)[1:].replace(',)', ''))
    for el in var[1]:
        track_res = connection.execute(track_req, el).fetchone()
        track_id_int = int(str(track_res)[1:].replace(',)', ''))
        data = (collection_id_int, track_id_int, )
        connection.execute(code, data)
