import os
import sys
import time
import matplotlib.pyplot as plt
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'lib')))
from workDB import create_tables, generate_data, insert_data, get_connection


CLEAR_ORDER = [
    "Ученик", "Группа_подготовки", "Наставник", "Преподаватель",
    "Курс", "Вид_подготовки", "Направление"
]


def clear_all_tables(db_name="online_school"):
    with get_connection(db_name) as conn:
        cursor = conn.cursor()
        for table in CLEAR_ORDER:
            cursor.execute(f"DELETE FROM {table}")
        conn.commit()


def measure_single_table(table_name, n_list, db_name="online_school"):
    times = []
    for n in n_list:
        clear_all_tables(db_name)
        start_time = time.time()
        data = generate_data(table_name, n)
        insert_data(table_name, data)
        end_time = time.time()
        times.append(end_time - start_time)
    return times


def measure_related_tables(parent_table, child_table, m, n_list, db_name="online_school"):
    times = []
    for n in n_list:
        clear_all_tables(db_name)
        start_time = time.time()
        parent_data = generate_data(parent_table, m)
        insert_data(parent_table, parent_data)
        if child_table == "Курс":
            naprav_data = generate_data("Направление", m)
            insert_data("Направление", naprav_data)
        child_data = generate_data(child_table, n)
        insert_data(child_table, child_data)
        end_time = time.time()
        times.append(end_time - start_time)
    return times


if __name__ == "__main__":
    db_name = "online_school"
    create_tables(db_name)
    n_list = [100, 200, 300, 500, 750, 1000, 2000, 5000, 8000, 10000]
    m = 100

    times_vid = measure_single_table("Вид_подготовки", n_list, db_name)
    times_pair = measure_related_tables("Вид_подготовки", "Курс", m, n_list, db_name)

    plt.figure(figsize=(10, 6))
    plt.plot(n_list, times_vid, label="Вид_подготовки")
    plt.plot(n_list, times_pair, label="(Вид_подготовки, Курс)")
    plt.title("Зависимость времени генерации и вставки данных от количества строк")
    plt.xlabel("Количество строк")
    plt.ylabel("Время (секунды)")
    plt.legend()
    plt.grid(True)
    plt.show()