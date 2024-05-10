"""
TODO
1. create DB, schema, and tables programatically
2. implement data loading using pandas,
3. implement data loading using pure python + pyodbc driver

AIM to do a real solution as you would implement in an actual engagement!

Deadline: Friday, 10th by EOBD
You will need to submit:
- evidence of having completed the hyperskill challenges
- the files produced to solve this technical challenge
"""
import datetime

import pandas as pd
import pyodbc


def create_connection():
    try:
        return pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                              'Server=localhost;'
                              'Database=DEA;'
                              'UID=sa;'
                              'PWD=n0m3l0s3.;')
    except:
        raise Exception("Error during creating connection")


def load_file(filename):
    csv_data = pd.read_csv(filename, on_bad_lines='warn')
    return pd.DataFrame(csv_data)


def build_params(row, headers):
    params = []
    for k in headers:
        params.append(row.__getattribute__(k))
    return params


def write_db(tablename, df, headers):
    con = create_connection()

    cursor = con.cursor()
    sql = f"INSERT INTO {tablename} VALUES({'?,' * (len(headers) - 1) + '?'})"
    for row in df.itertuples():
        try:
            cursor.execute(sql, build_params(row, headers))
            con.commit()
        except Exception as e:
            con.rollback()
            print(f"Error writting on db {row} error {e}")
    con.close()


def load_data(tablename, filename, headers):
    df = load_file(filename)
    write_db(tablename, df, headers)
    print(f"{tablename} Data Successfully Inserted")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_data('DEA.DBO.DEPARTMENT', 'data/DEPARTMENT.csv', ["DEPT_ID",
                                                            "DEPT_NM",
                                                            "DATE_CREATED",
                                                            "DATE_UPDATED"])
    load_data('DEA.DBO.POSITIONS', 'data/POSITIONS.csv', ["POS_ID",
                                                          "POS_NM",
                                                          "DATE_CREATED",
                                                          "DATE_UPDATED"])
    load_data('DEA.DBO.EMPLOYEE', 'data/EMPLOYEE_task.csv', ["EMP_ID",
                                                             "DEPT_ID",
                                                             "POS_ID",
                                                             "LAST_NAME",
                                                             "FIRST_NAME",
                                                             "EMAIL",
                                                             "MANAGER_ID",
                                                             "DATE_CREATED",
                                                             "DATE_UPDATED"])
    load_data('DEA.DBO.SALARY', 'data/SALARY_task.csv', ["SAL_ID",
                                                         "EMP_ID",
                                                         "SALARY_PER_HR",
                                                         "TAX",
                                                         "DATE_CREATED",
                                                         "DATE_UPDATED"])
