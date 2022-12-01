import dbf
from typing import Dict


"""
dbf таблица с вот такой структурой: SKLADNAIM,C,150	KOD,N,6,0	SKLKOD,C,3	ORG,C,150	INN,N,12,0
скрипт разбирает dbf таблицу, и группирует склады которые принадлежат одной организации, группировка на основе инн,
 группировка в виде: {буквенный код склада: цифровые коды складов-коллег} из родной организации
dbf table with this structure: SKLADNAIM,C,150 KOD,N,6,0 SKL KOD,C,3 ORG,C,150 IN,N,12,0
the script parses the dbf table, and groups warehouses that belong to the same organization, grouping based on information
and grouping in the form of a letter code warehouse: digital codes of warehouses-colleagues from the native organization
"""


# dict = {
#     '5902025531': [{
#         'kod': '96',
#         'sklkod': 'PV'
#     },{
#         'kod': '102',
#         'sklkod': 'PH'
#     },{
#         'kod': '886',
#         'sklkod': 'EG'
#     }
#
#     ]
# }


def read_dbf(f_name: str = '') -> Dict:
    """
    читаем наш dbf построчно, на выходе у нас будет словаь, ключами в словаре ИНН организации,
    значения ключей это список словарей [{буквенный код:, цифровой код:}, {буквенный код:, цифровой код:}]
    :param f_name: имя файла dbf
    :param f_name: str
    :return: dict
    """
    sklad_dict = {}
    f_dbf = dbf.Table(f_name)
    f_dbf.open(dbf.READ_ONLY)
    for line in f_dbf:
        inn = str(line.INN).strip()
        # если у склада нет буквенного кода, то присваиваем ему SKLAD
        if line.SKLKOD.strip() == '' and inn == '4300000000':
            sklkod = 'SKLAD'
        else:
            sklkod = line.SKLKOD.strip()
        i_dict = {
            'letter_kod': sklkod,
            'numeric_kod': str(line.KOD),
            'inn': str(line.INN)
        }

        if sklad_dict.get(inn, None) is not None:
            sklad_dict[inn].append(i_dict)
        else:
            sklad_dict[inn] = []
            sklad_dict[inn].append(i_dict)
    f_dbf.close()
    return sklad_dict


def consolidation_of_warehouses(i_dict: dict = {}) -> Dict:
    """
    на выходе у нас получается словарь вида 'BC': ';96;102;886;898;909;142;143;8;24;40;66;78;79;85;'
    ключ - буквенный код склада, а значения цифровые коды его коллег-складов из этой же организации
    :param i_dict: входной словарь из dbf файла ключ ИНН организации,
    а значение список[цифровой код склада, буквенный код склада, название ]
    :param i_dict: dict
    :return: dict буквенный код склада, и цифровые коды его коллег куда можно отправлять товар
    """
    o_dict = dict()
    for val in i_dict.values():
        o_dict.update({k: ';' + ';'.join(set(map(lambda x: x['numeric_kod'], val))) + ';' for k in
                       set(map(lambda x: x['letter_kod'], val))})
    out_dict = dict(sorted(o_dict.items(), key=lambda x: x[0]))
    return out_dict


def letter_kod_to_numeric_kod(i_dict={}) -> Dict:
    """
    на выходе у нас получается словарь вида 'BC': '96'
    ключ - буквенный код склада, а значение его цифровой код
    :param i_dict: входной словарь из dbf файла ключ ИНН организации,
    а значение список[цифровой код склада, буквенный код склада, название ]
    :param i_dict: dict
    :return: dict
    """
    o_dict = {}
    for val in i_dict.values():
        o_dict.update({k['letter_kod']: k['numeric_kod'] for k in val})
    out_dict = dict(sorted(o_dict.items(), key=lambda x: x[0]))
    return out_dict


def letter_kod_to_inn_kod(i_dict={}) -> Dict:
    """
    # на выходе у нас получается словарь вида 'BC': '5902025531'
    # ключ - буквенный код склада, а значение ИНН организации
    :param i_dict: входной словарь из dbf файла ключ ИНН организации,
    а значение список[цифровой код склада, буквенный код склада, название ]
    :param i_dict: dict
    :return: dict
    """
    o_dict = {}
    for val in i_dict.values():
        o_dict.update({k['letter_kod']: k['inn'] for k in val})
    out_dict = dict(sorted(o_dict.items(), key=lambda x: x[0]))
    return out_dict


def write_file(f_name, i_dict, heading, mode_open) -> None:
    """
    сохранение наших словарей в функции
    :param f_name: str имя файла куда будем сохранять
    :param i_dict: dict что будем сохранять
    :param heading: str заголовок псевдо функции сбис
    :param mode_open: str режим сохранения файла
    :return:
    """
    with open(f_name, mode_open, encoding='cp866') as file:
        file.write('Функция {}\n'.format(heading))
        file.write('{\n')
        file.write('ВыборПо(пКАСНОМ)\n')
        file.write('{\n')
        for key, val in i_dict.items():
            if len(val) < 220:
                file.write('Выбор "{}":Вернуть("{}")\n'.format(key, val))
            else:
                sep_start_index = val.find(';', 220)
                sep_finish_index = val.find(';', sep_start_index + 10)
                slice_s = val[sep_start_index:sep_finish_index]
                rezerv = val.partition(slice_s)
                file.write('Выбор "{}":Вернуть("{};")\n'.format(key, rezerv[0]))
                file.write('Выбор "{}":Вернуть("{}")\n'.format(key + '2', rezerv[1] + ';' + rezerv[2]))

        file.write('}\n')
        file.write('}\n')
        file.write('\n')


def main():
    file_name = 'ORG_SHOP.DBF'
    inn_org_sklad_dict = read_dbf(file_name)
    sklad_dict = consolidation_of_warehouses(i_dict=inn_org_sklad_dict)
    letter_to_numeric_dict = letter_kod_to_numeric_kod(i_dict=inn_org_sklad_dict)
    letter_to_inn_dict = letter_kod_to_inn_kod(i_dict=inn_org_sklad_dict)
    write_file('_massivskladi.prg', sklad_dict, '_МассивСкладыВыбор(пКАСНОМ)', 'w')
    write_file('_МассивСкладКод.prg', letter_to_numeric_dict, '_МассивСкладКод(пКАСНОМ)', 'w')
    write_file('_МассивСкладКодвИНН.prg', letter_to_inn_dict, '_МассивСкладКодвИНН(пКАСНОМ)', 'w')


if __name__ == '__main__':
    main()