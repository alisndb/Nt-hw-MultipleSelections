

CREATE TABLE if not exists Genre (
    id serial primary key,
    name varchar(20) not null
);

CREATE TABLE if not exists Author (
    id serial primary key,
    name varchar(40) not null
);

CREATE TABLE if not exists Collection (
    id serial primary key,
    name varchar(40) not null,
    year integer not null
);

CREATE TABLE if not exists Album (
    id serial primary key,
    name varchar(40) not null,
    year integer not null
);

CREATE TABLE if not exists Track (
    id serial primary key,
    album_id integer references Album(id),
    name varchar(40) not null,
    length integer not null
);

CREATE TABLE if not exists GenreAuthor (
    id serial primary key,
    genre_id integer references Genre(id),
    author_id integer references Author(id)
);

CREATE TABLE if not exists AlbumAuthor (
    id serial primary key,
    album_id integer references Album(id),
    author_id integer references Author(id)
);

CREATE TABLE if not exists CollectionTrack (
    id serial primary key,
    collection_id integer references Collection(id),
    track_id integer references Track(id)
);
