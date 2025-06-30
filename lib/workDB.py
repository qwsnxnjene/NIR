import mysql.connector
from contextlib import contextmanager
import random
import string
from datetime import datetime, timedelta
import csv
import os
from timeit import timeit
import matplotlib.pyplot as plt

@contextmanager
def get_connection(db_name="online_school"):
    conn = mysql.connector.connect(
        host="localhost",
        user="qwsnxnjene",
        password="root",
        database=db_name
    )
    try:
        yield conn
    finally:
        conn.close()

TABLES = {
    "Вид_подготовки": """
        CREATE TABLE IF NOT EXISTS Вид_подготовки (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
    """,
    "Направление": """
        CREATE TABLE IF NOT EXISTS Направление (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
    """,
    "Курс": """
        CREATE TABLE IF NOT EXISTS Курс (
            id INT AUTO_INCREMENT PRIMARY KEY,
            type_id INT,
            subject_id INT,
            name VARCHAR(100),
            FOREIGN KEY (type_id) REFERENCES Вид_подготовки(id),
            FOREIGN KEY (subject_id) REFERENCES Направление(id)
        )
    """,
    "Наставник": """
        CREATE TABLE IF NOT EXISTS Наставник (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fio VARCHAR(255) NOT NULL,
            started_at DATE,
            course_id INT,
            FOREIGN KEY (course_id) REFERENCES Курс(id)
        )
    """,
    "Преподаватель": """
        CREATE TABLE IF NOT EXISTS Преподаватель (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fio VARCHAR(255) NOT NULL,
            started_at DATE,
            course_id INT,
            FOREIGN KEY (course_id) REFERENCES Курс(id)
        )
    """,
    "Группа_подготовки": """
        CREATE TABLE IF NOT EXISTS Группа_подготовки (
            id INT AUTO_INCREMENT PRIMARY KEY,
            mentor_id INT,
            course_id INT,
            name VARCHAR(255) NOT NULL,
            FOREIGN KEY (mentor_id) REFERENCES Наставник(id),
            FOREIGN KEY (course_id) REFERENCES Курс(id)
        )
    """,
    "Ученик": """
        CREATE TABLE IF NOT EXISTS Ученик (
            id INT AUTO_INCREMENT PRIMARY KEY,
            fio VARCHAR(255) NOT NULL,
            group_id INT,
            mentor_id INT,
            FOREIGN KEY (group_id) REFERENCES Группа_подготовки(id),
            FOREIGN KEY (mentor_id) REFERENCES Наставник(id)
        )
    """
}

def create_tables(db_name="online_school"):
    with get_connection(db_name) as conn:
        cursor = conn.cursor()
        for table_name, sql in TABLES.items():
            cursor.execute(sql)
        conn.commit()


def random_word(length=10):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

def random_fio():
    first_names = ["Иван", "Петр", "Сергей", "Александр", "Мария", "Елена", "Ольга", "Анна"]
    middle_names = ["Иванович", "Петрович", "Сергеевич", "Александрович", "Ивановна", "Петровна", "Сергеевна", "Александровна"]
    last_names = ["Иванов", "Петров", "Сидоров", "Кузнецов", "Смирнова", "Попова", "Васильева", "Федорова"]
    return f"{random.choice(last_names)} {random.choice(first_names)} {random.choice(middle_names)}"

def random_date(start_date='-10y', end_date='today'):
    start = datetime.now() - timedelta(days=365*10) if start_date == '-10y' else datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.now() if end_date == 'today' else datetime.strptime(end_date, '%Y-%m-%d')
    random_days = random.randint(0, (end - start).days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')

def generate_data(table_name, n):
    if table_name == "Вид_подготовки":
        return [(random_word(),) for _ in range(n)]
    elif table_name == "Направление":
        return [(random_word(),) for _ in range(n)]
    elif table_name == "Курс":
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM Вид_подготовки")
            vid_ids = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT id FROM Направление")
            naprav_ids = [row[0] for row in cursor.fetchall()]
        return [(random.choice(vid_ids), random.choice(naprav_ids)) for _ in range(n)]
    elif table_name == "Наставник":
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM Курс")
            kurs_ids = [row[0] for row in cursor.fetchall()]
        return [(random_fio(), random_date(), random.choice(kurs_ids)) for _ in range(n)]
    elif table_name == "Преподаватель":
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM Курс")
            kurs_ids = [row[0] for row in cursor.fetchall()]
        return [(random_fio(), random_date(), random.choice(kurs_ids)) for _ in range(n)]
    elif table_name == "Группа_подготовки":
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM Наставник")
            nastavnik_ids = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT id FROM Курс")
            kurs_ids = [row[0] for row in cursor.fetchall()]
        return [(random.choice(nastavnik_ids), random.choice(kurs_ids), random_word()) for _ in range(n)]
    elif table_name == "Ученик":
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM Группа_подготовки")
            gruppa_ids = [row[0] for row in cursor.fetchall()]
            cursor.execute("SELECT id FROM Наставник")
            nastavnik_ids = [row[0] for row in cursor.fetchall()]
        return [(random_fio(), random.choice(gruppa_ids), random.choice(nastavnik_ids)) for _ in range(n)]
    else:
        raise ValueError(f"Неизвестная таблица: {table_name}")

def insert_data(table_name, data, db_name="online_school"):
    with get_connection(db_name) as conn:
        cursor = conn.cursor()
        if table_name == "Вид_подготовки":
            sql = "INSERT INTO Вид_подготовки (name) VALUES (%s)"
        elif table_name == "Направление":
            sql = "INSERT INTO Направление (name) VALUES (%s)"
        elif table_name == "Курс":
            sql = "INSERT INTO Курс (type_id, subject_id) VALUES (%s, %s)"
        elif table_name == "Наставник":
            sql = "INSERT INTO Наставник (fio, started_at, course_id) VALUES (%s, %s, %s)"
        elif table_name == "Преподаватель":
            sql = "INSERT INTO Преподаватель (fio, started_at, course_id) VALUES (%s, %s, %s)"
        elif table_name == "Группа_подготовки":
            sql = "INSERT INTO Группа_подготовки (mentor_id, course_id, name) VALUES (%s, %s, %s)"
        elif table_name == "Ученик":
            sql = "INSERT INTO Ученик (fio, group_id, mentor_id) VALUES (%s, %s, %s)"
        else:
            raise ValueError(f"Неизвестная таблица: {table_name}")
        cursor.executemany(sql, data)
        conn.commit()

def create_sandbox(main_db="online_school", sandbox_db="sandbox_db"):
    try:
        with get_connection(db_name=None) as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE IF EXISTS {sandbox_db}")
            cursor.execute(f"CREATE DATABASE {sandbox_db}")
        with get_connection(sandbox_db) as conn:
            create_tables(sandbox_db)
    except mysql.connector.Error as e:
        raise RuntimeError(f"Failed to create sandbox database: {e}")

def delete_data(table_name, db_name="online_school"):
    with get_connection(db_name) as conn:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()

def backup_table(table_name, file_path, db_name="online_school"):
    with get_connection(db_name) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([i[0] for i in cursor.description])
            writer.writerows(cursor.fetchall())

def restore_table(table_name, file_path, db_name="online_school"):
    with get_connection(db_name) as conn:
        cursor = conn.cursor()
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            sql = f"INSERT INTO {table_name} ({','.join(headers)}) VALUES ({','.join(['%s'] * len(headers))})"
            for row in reader:
                cursor.execute(sql, row)
        conn.commit()

def measure_time(query, db_name="online_school"):
    with get_connection(db_name) as conn:
        cursor = conn.cursor()

        def execute_query():
            cursor.execute(query)
            cursor.fetchall()

        time = timeit(execute_query, number=1)
    return time

def plot_graph(x_data, y_data_list, labels, title, x_label, y_label, save_path):
    plt.figure()
    for y_data, label in zip(y_data_list, labels):
        plt.plot(x_data, y_data, label=label)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend()
    plt.savefig(save_path)
    plt.close()

if __name__ == "__main__":
    create_tables()
    for table in TABLES:
        data = generate_data(table, 10)
        insert_data(table, data)