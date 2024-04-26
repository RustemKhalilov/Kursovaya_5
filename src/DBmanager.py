import psycopg2

from src.config import config

class DBManager:
    def __init__(self):
        self.db_name = 'hh_company'

    def execute_(self, query):
        conn = psycopg2.connect(dbname=self.db_name, **config())
        with conn:
            with conn.cursor() as cur:
                cur.execute(query)
                results = cur.fetchall()
        conn.close()
        return results

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании."""
        result = self.execute_(f"SELECT employers.employer_name, COUNT(vacanciya.employer_Id) AS employer "
                               f"FROM vacanciya, employers "
                               f"WHERE (employers.employer_ID=vacanciya.employer_ID) "
                               f"GROUP BY employers.employer_name")

        return result

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на
        вакансию."""
        result = self.execute_(f'SELECT vacanciya_name, employer_name, salary_down, salary_up, url '
                               f'FROM vacanciya, employers '
                               f'WHERE (employers.employer_ID=vacanciya.employer_ID)')
        return result

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям."""
        result = self.execute_(f'SELECT AVG(salary_down) AS "Средняя зарплата ОТ",  AVG(salary_up) AS "Средняя зарплата ДО" FROM vacanciya '
                               f'WHERE salary_down > 0 OR salary_up > 0')
        return result

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        result = self.execute_(f'SELECT * FROM vacanciya '
                               f'WHERE salary_up >(SELECT AVG(salary_up) FROM vacanciya) AND salary_down > (SELECT AVG(salary_down) FROM vacanciya) '
                               f'ORDER BY vacanciya_id ')
        return result

    def get_vacancies_with_keyword(self):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python."""
        result = self.execute_(f"SELECT * "
                               f"FROM vacanciya "
                               f"WHERE vacanciya_name LIKE '%инженер%'")
        return result