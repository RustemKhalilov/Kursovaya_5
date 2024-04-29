from src.utils import HhunterApi, create_database, delete_database, add_tables, res_vacanciya_to_db
from src.DBmanager import DBManager


def creation_and_filling_DB():
    employer_dict = {
        '5554827': 'АО ГМС Нефтемаш',
        '1808': 'НОВАТЭК',
        '241482': 'ООО ИПИГАЗ',
        '1013600': 'ПАО Гипротюменнефтегаз',
        '1572374': 'АО Транснефть - Сибирь',
        '1333787': 'ООО Тюменьнефтегазпроект',
        '1898425': 'ООО Газпром проектирование',
        '5093397': 'ООО Якутгазпроект',
        '1088373': 'Тюменский Государственный Универститет',
        '2921408': 'НИПИГАЗ'
    }

    employer_list = list(employer_dict.values())

    my_vacancuya_list = []
    for key, value in employer_dict.items():
        my_vac_list = HhunterApi().get_vacancies(20, key)
        my_vacancuya_list.append(my_vac_list)

    my_vacancuya_list_out = []
    for index, item in enumerate(my_vacancuya_list):
        my_vacancuya_dict_temp = []
        for item2 in item['items']:
            my_dict = {
                'наименование вакансии': item2['name'],
                'профессиональные навыки': item2['professional_roles'][0]['name']
            }
            if item2['address'] is None:
                my_dict['адрес'] = 'нет данных'
            else:
                my_dict['адрес'] = f"{item2['address']['street']} {item2['address']['building']}"

            if item2['salary'] is None:
                my_dict['зарплата максимальная'] = 0
                my_dict['зарплата минимальная'] = 0
            else:
                if 'from' in item2['salary']:
                    my_dict['зарплата минимальная'] = item2['salary']['from']
                if 'to' in item2['salary']:
                    my_dict['зарплата максимальная'] = item2['salary']['to']

            if item2['url'] is None:
               my_dict['электронный адрес'] = 'Нет электронного адреса'
            else:
               my_dict['электронный адрес'] = item2['url']

            my_vacancuya_dict_temp.append(my_dict)
        my_vacancuya_list_out.append([employer_list[index], my_vacancuya_dict_temp])

    delete_database('hh_company')
    create_database('hh_company')
    add_tables()
    res_vacanciya_to_db('hh_company', my_vacancuya_list_out)


if __name__ == "__main__":
    creation_and_filling_DB()
    My_DBManager = DBManager()
    vacanc_cont = My_DBManager.get_companies_and_vacancies_count()
    [print(index, x[0], x[1]) for index, x in enumerate(vacanc_cont)]
    print('Конец первого запроса')
    print('*************************************')
    print('*************************************')
    vacanc_cont = My_DBManager.get_all_vacancies()
    [print(index, x[0], ", ", x[1], ", ", x[2], ", ", x[3], ", ", x[4]) for index, x in enumerate(vacanc_cont)]
    print('Конец второго запроса')
    print('*************************************')
    print('*************************************')
    vacanc_cont = My_DBManager.get_avg_salary()
    [print(index, 'Средняя зарплата от:', x[0], 'Средняя зарплата до:', x[1]) for index, x in enumerate(vacanc_cont)]
    print('Конец третьего запроса')
    print('*************************************')
    print('*************************************')
    vacanc_cont = My_DBManager.get_vacancies_with_higher_salary()
    [print(index, x[0], ", ", x[1], ", ", x[2], ", ", x[3], ", ", x[4], ", ", x[5], ", ", x[6], ", ", x[7]) for index, x in enumerate(vacanc_cont)]
    print('Конец четвертого запроса')
    print('*************************************')
    print('*************************************')
    vacanc_cont = My_DBManager.get_vacancies_with_keyword()
    [print(index, x[0], ", ", x[1], ", ", x[2], ", ", x[3], ", ", x[4], ", ", x[5], ", ", x[6], ", ", x[7]) for index, x
     in enumerate(vacanc_cont)]

