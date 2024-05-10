import csv

from pandas_app import create_connection


def read_file(filename: str):
    rows = []
    with open(filename, 'r', encoding="utf8") as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        print(headers)
        for i, row in enumerate(csv_reader):
            if len(row) != len(headers):
                print(f"Skipping line {i}: expected {len(headers)} fields, saw {len(row)}")
            rows.append(row)
    return headers, rows


def write_db(tablename, rows, headers):
    con = create_connection()

    cursor = con.cursor()
    sql = f"INSERT INTO {tablename} VALUES({'?,' * (len(headers) - 1) + '?'})"
    for row in rows:
        try:
            cursor.execute(sql, row)  # TODO scape characters
            con.commit()
        except Exception as e:
            con.rollback()
            print(f"Error writting on db {row} error {e}")
    con.close()


def load_data(tablename, filename):
    headers, rows = read_file(filename)
    write_db(tablename, rows, headers)
    print(f"{tablename} Data Successfully Inserted")


if __name__ == '__main__':
    load_data('DEA.DBO.DEPARTMENT', 'data/DEPARTMENT.csv')
    load_data('DEA.DBO.POSITIONS', 'data/POSITIONS.csv')
    load_data('DEA.DBO.EMPLOYEE', 'data/EMPLOYEE_task.csv')
    load_data('DEA.DBO.SALARY', 'data/SALARY_task.csv')
