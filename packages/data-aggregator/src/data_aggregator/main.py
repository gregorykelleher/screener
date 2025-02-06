# data_aggregator/main.py

from .equities_aggregator import populate_equities_db


def main():
    populate_equities_db()


if __name__ == "__main__":
    main()
