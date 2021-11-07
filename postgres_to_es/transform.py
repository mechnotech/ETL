from data_classes import MovieRaw


def _person_formatter(persons: dict) -> list:
    output = [{'id': k, 'name': v} for k, v in persons.items()]
    return output


def transformer(movie_to_transform: list) -> tuple:
    """
    Принимает список списков денормализованных данных по одному фильму
    собирает и отдаёт doc и uuid для ES
    """

    director = set()
    genre = set()
    actor_names = set()
    writer_names = set()
    actors = dict()
    writers = dict()

    for row in movie_to_transform:

        data = MovieRaw(*row)

        if data.role == 'director':
            director.add(data.person_name)
        elif data.role == 'writer':
            writer_names.add(data.person_name)
            writers[data.person_id] = data.person_name
        elif data.role == 'actor':
            actor_names.add(data.person_name)
            actors[data.person_id] = data.person_name

    director = None if not director else ', '.join(director)
    genre = None if not genre else ', '.join(genre)
    actor_names = None if not actor_names else ', '.join(actor_names)
    writer_names = None if not writer_names else list(writer_names)

    doc = {
        'id': data.uuid,
        'imdb_rating': data.imdb_rating,
        'genre': genre,
        'title': data.title,
        'description': data.description,
        'director': director,
        'actors_names': actor_names,
        'writers_names': writer_names,
        'actors': _person_formatter(actors),
        'writers': _person_formatter(writers),

    }
    return doc, data.uuid


if __name__ == '__main__':
    pass
