def movies_fixtures(body: list):
    with open('misc/movies_fixtures.txt', 'a') as f:
        f.writelines(body)


def genres_fixtures(body: list):
    with open('misc/genres_fixtures.txt', 'a') as f:
        f.writelines(body)


def persons_fixtures(body: list):
    with open('misc/persons_fixtures.txt', 'a') as f:
        f.writelines(body)


if __name__ == '__main__':
    pass
