import dbf

"""
dbf таблица с вот такой структурой: SKLADNAIM,C,150	KOD,N,6,0	SKLKOD,C,3	ORG,C,150	INN,N,12,0
скрипт разбирает dbf таблицу, и группирует склады которые принадлежат одной организации, группировка на основе инн
а группировка в виде буквенный код склада: цифровые коды складов-коллег из родной организации
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
def read_dbf(f_name):
# читаем наш dbf построчно, на выходе у нас будет словаь, ключами в словаре ИНН организации,
# значения ключей это список словарей {буквенный код:, цифровой код:}
    sklad_dict = {}
    f_dbf = dbf.Table(f_name)
    f_dbf.open(dbf.READ_ONLY)
    for line in f_dbf:
        # если у склада нет буквенного кода, то присваиваем ему SKLAD
        if line.SKLKOD.strip() == '':
            sklkod = 'SKLAD'
        else:
            sklkod = line.SKLKOD.strip()
        i_dict = {
            'letter_kod': sklkod,
            'numeric_kod': str(line.KOD),
        }
        inn = str(line.INN).strip()
        if sklad_dict.get(inn, None) is not None:
            sklad_dict[inn].append(i_dict)
        else:
            sklad_dict[inn] = []
            sklad_dict[inn].append(i_dict)
    f_dbf.close()
    return sklad_dict

def letter_kod_all_numeric_kod(list_numeric=[]):
    s = ';'
    #собираем из "значения ключей это список словарей {буквенный код:, цифровой код:}"
    # строку чисто с цифровыми кодами, разделитель точка с запятой
    for elem in list_numeric:
        s += str(elem['numeric_kod']) + ';'
        # если строка получилась слтшком большая, то вставляем в нее сепаратор, в будущем по нему сделаем разбивку
        if len(s) > 230 and s.find('separator') == -1:
            s += ';separator;'
    return s

def consolidation_of_warehouses(i_dict={}):
    o_dict = {}
    # на выходе у нас получается словарь вида BC: ;96;102;886;898;909;142;143;8;24;40;66;78;79;85;;
    # ключ - буквенный код склада, а значения цифровые коды его коллег-складов из этой же организации
    for key, val in i_dict.items():
        for k in val:
            o_dict[k['letter_kod']] = letter_kod_all_numeric_kod(val)
            # если нашли в нашей строке сепаратор, то разбиваем по нему строку и добавляем новое значение в выходной словарь
            if o_dict[k['letter_kod']].find(';separator;') != -1:
                rezerv = o_dict[k['letter_kod']].partition(';separator;')
                o_dict[k['letter_kod']] = rezerv[0]
                o_dict[k['letter_kod'] + '2'] = ';' + rezerv[2]
    out_dict = dict(sorted(o_dict.items(), key=lambda x: x[0]))
    return out_dict

def write_file(f_name, i_dict):
    with open(f_name, 'w', encoding='cp866') as file:
        file.write('Функция _МассивСкладыВыбор(пКАСНОМ)\n')
        file.write('{\n')
        file.write('ВыборПо(пКАСНОМ)\n')
        file.write('{\n')
        for key, val in i_dict.items():
            file.write(f'Выбор "{key}":Вернуть("{val}")\n')
        file.write('}\n')
        file.write('}\n')

file_name = 'ORG_SHOP.DBF'
inn_ord_sklad_list = read_dbf(file_name)
sklad_dict = consolidation_of_warehouses(i_dict=inn_ord_sklad_list)
write_file('_massivskladi.prg', sklad_dict)