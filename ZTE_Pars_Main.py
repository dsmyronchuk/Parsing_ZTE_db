import pandas as pd
import paramiko
import zipfile
import io
from sqlalchemy import create_engine
import sqlalchemy
from secret import sql_path, hostname, username, password
import fsspec       # Lib for exe

# Connect to SQL BD artint(BSS)

SQL_path = sql_path
SQL_db = create_engine(SQL_path)

# Connect to SFTP  Server
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=hostname, username=username, password=password)
ftp_client = ssh_client.open_sftp()


def col_length_max(name):                                    # Функция для подбора количества символов для Varchar
    length_set = set()
    df[name].map(lambda x: length_set.add(len(str(x))))
    length_type = 1
    if len(length_set) > 0:
        length_type = max(length_set)
    return length_type


def count_int(name):                                         # Функция для подбора количества символов int
    length_set = set()
    df[name].map(lambda x: length_set.add(str(x)))
    count_int_or_bigint = 0
    count_max = 0
    for i in length_set:
        if int(i) > count_max:
            count_max = int(i)
    if count_max > 2147483647:
        count_int_or_bigint = 1
    return count_int_or_bigint


def mapping_df_types(data_frame):                               # Функция для подбора нужных типов данных в SQL
    dtypedict = {}
    for i, j, w in zip(data_frame.columns, data_frame.dtypes, data_frame):
        if "object" in str(j):
            w_length = col_length_max(w)
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=w_length)})
        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=2, asdecimal=True)})
        if "int" in str(j):
            if count_int(w) == 0:
                dtypedict.update({i: sqlalchemy.types.Integer()})
            else:
                dtypedict.update({i: sqlalchemy.types.BigInteger()})
    return dtypedict


path_folfer = "/home/omc/naf/ums-server/rundata/ppus/minos.ppu/minos-naf.pmu/nop"

with ftp_client.open(path_folfer, "r", bufsize=32768) as f:
    folder_need = []
    folder_time = 0
    for nop in ftp_client.listdir(path_folfer):     # Вывожу список папок в nop
        nop_folder = path_folfer + '/' + nop        # Склеиваю путь к папкам в nop

        with ftp_client.open(nop_folder, "r", bufsize=32768) as f1:      # захожу в папки внутри nop
            folder_need = []
            folder_time = 0
            for entry in ftp_client.listdir_attr(nop_folder):           # узнаю максимальное время
                if entry.st_mtime >= folder_time:
                    folder_time = entry.st_mtime

            for entry in ftp_client.listdir_attr(nop_folder):   # Если максимальное время совпадает с текущей папкой,
                if entry.st_mtime == folder_time:               # то добавить название папки в переменную folder_time
                    folder_need.append(entry.filename)

            for number in folder_need:                          # Прохожусь по полученному списку из названий файлов
                path_folder1 = nop_folder + '/' + number        # Склеиваю путь к нужным ZIP с CSV
                with ftp_client.open(path_folder1, "r", bufsize=32768) as f2:       # Читаю Zip
                    archive = zipfile.ZipFile(f2, 'r')
                    for i in archive.namelist():                # Прохожусь циклом по CSV файлам из Zip
                        df = pd.read_csv(io.BytesIO(archive.read(i)), encoding="ISO-8859-1")
                        dtypedict = mapping_df_types(df)
                        Name_table = 'ZTE_' + nop + '_' + i.split('.')[0]
                        if len(df) > 1:
                            print(i)
                            df.to_sql(
                                Name_table, con=SQL_db, schema='BSS', if_exists='replace', dtype=dtypedict, index=False
                                     )
