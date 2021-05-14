import psycopg2
from contextlib import closing
import numpy as np
import pandas as pd


class DataBaseAdaptor:
    def __init__(self):
        self.db = "db"
        self.user = "postgres"
        self.password = "postgres"
        self.host = "host"

    def products_save(self, first_colomn, second, third, fourth, fifth):    # Save page content to data index

        with closing(self.get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO product (price, relevance, prevalence) "
                    " VALUES (%(factory_code)s, %(name)s, %(manufact_code)s, %(price)s, %(relevance)s) "
                    " RETURNING id ",
                    {
                        "factory_code": first_colomn,
                        "name": second,
                        "manufact_code": third,
                        "price": fourth,
                        "relevance": fifth
                    }
                )

                conn.commit()

    def manufacturer_save(self, first_colomn, second, third, fourth):    # Save page content to data index

        with closing(self.get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO product (price, relevance, prevalence) "
                    " VALUES (%(name)s, %(dev_level)s, %(manufact_code)s, %(price)s, %(relevance)s) "
                    " RETURNING id ",
                    {
                        "dev_level": first_colomn,
                        "name": second,
                        "manufact_code": third,
                        "price": fourth
                    }
                )

                conn.commit()


    def countries_save(self, first_colomn, second, third, fourth):    # Save page content to data index

        with closing(self.get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO product (price, relevance, prevalence) "
                    " VALUES (%(name)s, %(dev_level)s, %(manufact_code)s, %(price)s, %(relevance)s) "
                    " RETURNING id ",
                    {
                        "dev_level": first_colomn,
                        "name": second,
                        "manufact_code": third,
                        "price": fourth
                    }
                )

                conn.commit()

    def factors_save(self, first_colomn, second, third, fourth):    # Save page content to data index

        with closing(self.get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO product (price, relevance, prevalence) "
                    " VALUES (%(name)s, %(dev_level)s, %(manufact_code)s, %(price)s, %(relevance)s) "
                    " RETURNING id ",
                    {
                        "dev_level": first_colomn,
                        "name": second,
                        "manufact_code": third,
                        "price": fourth
                    }
                )

                conn.commit()

    def prices_save(self, first_colomn, second, third, fourth):    # Save page content to data index

        with closing(self.get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO product (price, relevance, prevalence) "
                    " VALUES (%(name)s, %(dev_level)s, %(manufact_code)s, %(price)s, %(relevance)s) "
                    " RETURNING id ",
                    {
                        "dev_level": first_colomn,
                        "name": second,
                        "manufact_code": third,
                        "price": fourth
                    }
                )

                conn.commit()

    def test_execute(self):    # Database version
        with closing(self.get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version()")

    def get_connection(self):     # connection factory
        return psycopg2.connect(
            dbname=self.db,
            user=self.user,
            password=self.password,
            host=self.host
        )


if __name__ == "__main__":
    db_adaptor = DataBaseAdaptor()
    product = pd.read_csv('c:/Users/Dmitry/Desktop/product.csv')

    for index, row in product.iterrows():
        DataBaseAdaptor.products_save(row[0], row[1], row[2], row[3], row[4])


    countries = pd.read_csv('countries.csv', delimiter=';')   #names2 = ['my_datetime', 'event', 'country', 'user_id', 'source', 'topic']

    for index, row in countries.iterrows():
        DataBaseAdaptor.countries_save(row[0], row[1], row[2], row[3], row[4])

    factors = pd.read_csv('factors.csv', delimiter=';')

    for index, row in factors.iterrows():
        DataBaseAdaptor.factors_save(row[0], row[1], row[2], row[3], row[4])

    prices = pd.read_csv('prices.csv', delimiter=';')

    for index, row in prices.iterrows():
        DataBaseAdaptor.prices_save(row[0], row[1], row[2], row[3], row[4])

    manufacturer = pd.read_csv('manufacturer.csv', delimiter=';')

    for index, row in manufacturer.iterrows():
        DataBaseAdaptor.manufacturer_save(row[0], row[1], row[2], row[3], row[4])
