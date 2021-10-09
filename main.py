# coding:utf-8
import csv
import MySQLdb
import os
import datetime

# 挿入処理
def inserter(filename, table):
    print("[Insert]")
    # インサート元のパスを生成
    filepath = "./" + filename + ".csv"
    # データベースと接続
    connection = MySQLdb.connect(
        db="",
        user="root",
        passwd="",
        charset="utf8mb4"
    )
    # エラー数の初期化
    error = 0
    # エラーログ出力先ディレクトリの指定
    err_dir = dest_dir = './error/inserter/' + filename
    # エラーログ出力先ファイルの指定
    err_log = err_dir + '/log'
    # エラー出力先ディレクトリの作成
    os.makedirs(dest_dir, exist_ok=True)
    # インサートの実行
    print(filepath+" is inserting.")
    # クエリの準備
    cursor=connection.cursor()
    try:
        with open(filepath, "r", encoding="cp932") as f:
            reader = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            header = next(reader)
            l = [row for row in reader]
            for row in l:
                if(row[0]==''):
                    break
                try:
                    if(row[0]=='普通' or row[0]=='その他'):
                        sql = "INSERT INTO " + table + " values('0','" + row[1] + "','" + row[2] + "','" + row[4] + "');"
                        cursor.execute(sql)
                except MySQLdb._exceptions.ProgrammingError as e:
                    out_err(err_log, e, sql)
                    error += 1
                    continue
                except MySQLdb._exceptions.OperationalError as e:
                    out_err(err_log, e, sql)
                    error += 1
                    continue
    except FileNotFoundError as e:
        print(e)
    except csv.Error as e:
        print(e)
    connection.commit()
    cursor.close()
    connection.close()
    if error > 0:
        print("\033[33m" + "error(" + str(error) + ") See -> " + err_log + "\033[0m")
    print('\033[32m' + "Inserted!" + '\033[0m')

# エラー出力
def out_err(err_log, e, sql):
    with open (err_log, "a") as err_out:
        err_time = datetime.datetime.now()
        err_out.write("[" + str(err_time) + "]" + str(e) + "\n" + sql + "\n")
    err_out.close()


if __name__ == '__main__':
    # print("Filename?：", end="")
    # filename = input()
    # print("Table?：", end="")
    # table = input()
    filename = "202104260946"
    table = "onsens"
    inserter(filename, table)
