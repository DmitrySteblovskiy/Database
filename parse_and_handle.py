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

    def data_parse(self, data):
        data = pd.read_csv('cat_exam_data.csv')
        data.head(5)
        exam_data.isna().mean()
        exam_data.describe()
    def test_execute(self):    # Database version
        with closing(self.get_connection()) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT version()")
    # Среднее арифметическое считается просто (разделили сумму на количество), медиана - центр отсортированной последовательности, а мода - по сути самый встречающийся элемент.
def print_all_means(name, data):
    print(name + ':')
    print("Матожиданиe: ", data.mean())
    print("Медиана: ", data.median())
    print("Мода: ", data.mode()[0])

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

    # теперь по очереди для каждого поля выведем результаты
    print_all_means("school", exam_data.school)
    print("")
    print_all_means("test_score", exam_data.test_score)
    print("")
    print_all_means("number_of_students", exam_data.number_of_students)
