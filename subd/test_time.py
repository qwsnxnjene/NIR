import matplotlib.pyplot as plt
import time
import os
from timeit import timeit
from mainSUBD import Table, Column  # Предполагается, что у тебя есть эти классы

# Функция для создания таблицы с N записями
def create_table_with_records(table_name, columns, N):
    table = Table(table_name, columns)
    for i in range(N):
        if 'id' in [col.name for col in columns]:
            table.insert([i, f"Data_{i}"])
        else:
            table.insert([f"Data_{i}"])
    return table

# Функция для измерения времени вставки N записей
def measure_insert(columns, N):
    table_name = "temp_insert"
    start = time.time()
    table = create_table_with_records(table_name, columns, N)
    end = time.time()
    # Очистка файлов
    os.remove(table.data_file)
    os.remove(table.schema_file)
    for idx_file in table.index_files.values():
        os.remove(idx_file)
    return end - start

# Функция для измерения времени выборки
def measure_select(table, where_condition, repeats=100):
    def select_op():
        table.select(where=where_condition)
    times = timeit(select_op, number=repeats)
    return times / repeats

# Значения N для тестирования
N_values = [1000, 5000, 10000]

# Списки для хранения результатов
insert_times_with_index = []
insert_times_without_index = []
select_times_with_index = []
select_times_without_index = []
delete_times_with_index = []
delete_times_without_index = []

# Определение колонок для таблиц
columns_with_int = [Column('id', 'INT'), Column('data', 'VARCHAR', 50)]
columns_without_int = [Column('data', 'VARCHAR', 50)]

# Выполнение тестов
for N in N_values:
    # Тест вставки
    time_with = measure_insert(columns_with_int, N)
    time_without = measure_insert(columns_without_int, N)
    insert_times_with_index.append(time_with)
    insert_times_without_index.append(time_without)

    # Тест выборки
    table_a = create_table_with_records("table_a", columns_with_int, N)
    select_time_with = measure_select(table_a, ('id', '=', N//2), repeats=1000)
    select_times_with_index.append(select_time_with)

    table_b = create_table_with_records("table_b", columns_without_int, N)
    select_time_without = measure_select(table_b, ('data', '=', f"Data_{N//2}"), repeats=1000)
    select_times_without_index.append(select_time_without)

    # Тест удаления
    table_a = create_table_with_records("table_a", columns_with_int, N)
    start = time.time()
    table_a.delete(where=('id', '=', N//2))
    delete_time_with = time.time() - start
    delete_times_with_index.append(delete_time_with)

    table_b = create_table_with_records("table_b", columns_without_int, N)
    start = time.time()
    table_b.delete(where=('data', '=', f"Data_{N//2}"))
    delete_time_without = time.time() - start
    delete_times_without_index.append(delete_time_without)

# Построение графиков
plt.figure(figsize=(12, 8))

# График для вставки
plt.subplot(3, 1, 1)
plt.plot(N_values, insert_times_with_index, label='С индексом')
plt.plot(N_values, insert_times_without_index, label='Без индекса')
plt.title('Время вставки vs N')
plt.xlabel('N (количество записей)')
plt.ylabel('Время (с)')
plt.legend()

# График для выборки
plt.subplot(3, 1, 2)
plt.plot(N_values, select_times_with_index, label='С индексом')
plt.plot(N_values, select_times_without_index, label='Без индекса')
plt.title('Время выборки vs N')
plt.xlabel('N (количество записей)')
plt.ylabel('Время (с)')
plt.legend()

# График для удаления
plt.subplot(3, 1, 3)
plt.plot(N_values, delete_times_with_index, label='С индексом')
plt.plot(N_values, delete_times_without_index, label='Без индекса')
plt.title('Время удаления vs N')
plt.xlabel('N (количество записей)')
plt.ylabel('Время (с)')
plt.legend()

plt.tight_layout()
plt.show()

# Очистка временных файлов (опционально)
for table_name in ["table_a", "table_b", "temp_insert"]:
    if os.path.exists(f"{table_name}.dat"):
        os.remove(f"{table_name}.dat")
    if os.path.exists(f"{table_name}.schema.json"):
        os.remove(f"{table_name}.schema.json")