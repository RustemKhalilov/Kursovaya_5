import requests
import psycopg2
from psycopg2 import Error
from abc import ABC, abstractmethod
from src.config import config


class Apihh(ABC):

    @abstractmethod
    def get_vacancies(self):
        pass


class HhunterApi(Apihh):
    def __init__(self):
        self.base_url = "https://api.hh.ru/vacancies"

    def get_vacancies(self, vacans_cont, employee_id):
        # params = {"employer_id": ID организации, "area": 95}  # Здесь 95 - это код региона Тюмень
        # # Справочник для параметров GET-запроса
        params = {
            "employer_id": employee_id,
            'area': 95,  # Поиск осуществляется по вакансиям города Тюмень
            'per_page': vacans_cont  # Кол-во вакансий на 1 странице
        }
        response = requests.get(self.base_url, params=params)
        return response.json()



def database_command(database_name, command):
    """
       функция выполняет запросы к PostgreSQL
    """
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(dbname=database_name, **config())
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        connection.autocommit = True
        cursor.execute(command)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def create_database(database_name):
    """
      Создание базы данных hh_company
    """
    try:
        connection = psycopg2.connect(**config())
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f'CREATE DATABASE {database_name}')
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")

def delete_database(database_name):
    """
      Удаление базы данных hh_company
    """
    try:
        connection = psycopg2.connect(**config())
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(f'DROP DATABASE {database_name}')
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")

def add_tables():
    """Добавляет таблицы в базу данных."""

    commamd = (f'CREATE TABLE employers'
               f'(employer_ID int PRIMARY KEY,'
               f'employer_name text)')

    database_command('hh_company', commamd)

    commamd = (f'CREATE TABLE vacanciya'
               f'(vacanciya_id int PRIMARY KEY,'
               f'vacanciya_name text,'
               f'salary_up real,'
               f'salary_down real,'
               f'professional_roles text,'
               f'address text,'
               f'url text,'
               f'employer_ID int)')

    database_command('hh_company', commamd)

def res_vacanciya_to_db(database_name:str, my_list:dict):
    """
       функция заполняет таблицы базы данных hh_company
    """
    try:
        # Подключение к существующей базе данных
        connection = psycopg2.connect(dbname=database_name, **config())
        # Курсор для выполнения операций с базой данных
        cursor = connection.cursor()
        connection.autocommit = True
        postgres_insert_query_1 = """ INSERT INTO employers (employer_ID, employer_name)
                                               VALUES (%s,%s)"""
        postgres_insert_query_2 = """ INSERT INTO vacanciya (vacanciya_id, vacanciya_name, salary_up, salary_down, professional_roles, address, url, employer_ID)
                                                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
        counter = 0
        for index, item in enumerate(my_list):
            cursor.execute(postgres_insert_query_1, [index, item[0]])
            for index2, item2 in enumerate(item[1]):
                cursor.execute(postgres_insert_query_2, [counter,
                                                              item2['наименование вакансии'],
                                                              item2['зарплата максимальная'],
                                                              item2['зарплата минимальная'],
                                                              item2['профессиональные навыки'],
                                                              item2['адрес'],
                                                              item2['электронный адрес'],
                                                              index])
                counter += 1



    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")

#             'наименование вакансии': item.name,
#             'зарплата максимальная': item.salary_up,
#             'минимальная зарплата': item.salary_down,
#             'адрес': item.adress,
#             'электронный адрес': item.alternate_url,
#             'обязанности': item.snippet,
#             'профессиональные навыки': item.professional_roles
