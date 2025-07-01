import matplotlib.pyplot as plt
import time
import os
from timeit import timeit
from mainSUBD import Database, Column


def create_table_with_records(db, table_name, columns, N):
    columns_def = ', '.join(
        f"{col.name} {col.type}" + (f"({col.length})" if col.type == 'VARCHAR' else '')
        for col in columns
    )
    db.execute(f"CREATE TABLE {table_name} ({columns_def})")

    for i in range(N):
        if 'id' in [col.name for col in columns]:
            db.execute(f"INSERT INTO {table_name} VALUES ({i}, 'Data_{i}')")
        else:
            db.execute(f"INSERT INTO {table_name} VALUES ('Data_{i}')")


def measure_insert(db, table_name, columns, N):
    start = time.time()
    create_table_with_records(db, table_name, columns, N)
    end = time.time()
    # удаляем файлы после замеров
    if os.path.exists(f"{table_name}.dat"):
        os.remove(f"{table_name}.dat")
    if os.path.exists(f"{table_name}.schema.json"):
        os.remove(f"{table_name}.schema.json")
    for col in columns:
        if col.type == 'INT':
            idx_file = f"{table_name}_{col.name}.idx"
            if os.path.exists(idx_file):
                os.remove(idx_file)
    return end - start


def measure_select(db, table_name, where_condition, repeats=100):
    def select_op():
        db.execute(f"SELECT * FROM {table_name} WHERE {where_condition[0]} = {where_condition[2]!r}")

    times = timeit(select_op, number=repeats)
    return times / repeats


def measure_delete(db, table_name, where_condition):
    start = time.time()
    db.execute(f"DELETE FROM {table_name} WHERE {where_condition[0]} = {where_condition[2]!r}")
    end = time.time()
    return end - start


N_values = [1000, 5000, 10000]

insert_times_with_index = []
insert_times_without_index = []
select_times_with_index = []
select_times_without_index = []
delete_times_with_index = []
delete_times_without_index = []

columns_with_int = [Column('id', 'INT'), Column('data', 'VARCHAR')]
columns_without_int = [Column('data', 'VARCHAR')]

for N in N_values:
    # замеряем insert
    db = Database()
    time_with = measure_insert(db, "table_with_index", columns_with_int, N)
    insert_times_with_index.append(time_with)

    db = Database()
    time_without = measure_insert(db, "table_without_index", columns_without_int, N)
    insert_times_without_index.append(time_without)
    # замеряем select
    db = Database()
    create_table_with_records(db, "table_a", columns_with_int, N)
    select_time_with = measure_select(db, "table_a", ('id', '=', N // 2), repeats=1000)
    select_times_with_index.append(select_time_with)

    db = Database()
    create_table_with_records(db, "table_b", columns_without_int, N)
    select_time_without = measure_select(db, "table_b", ('data', '=', f"Data_{N // 2}"), repeats=1000)
    select_times_without_index.append(select_time_without)

    # замеряем delete
    db = Database()
    create_table_with_records(db, "table_a", columns_with_int, N)
    delete_time_with = measure_delete(db, "table_a", ('id', '=', N // 2))
    delete_times_with_index.append(delete_time_with)

    db = Database()
    create_table_with_records(db, "table_b", columns_without_int, N)
    delete_time_without = measure_delete(db, "table_b", ('data', '=', f"Data_{N // 2}"))
    delete_times_without_index.append(delete_time_without)

plt.figure(figsize=(12, 8))
plt.subplot(3, 1, 1)
plt.plot(N_values, insert_times_with_index, label='С индексом')
plt.plot(N_values, insert_times_without_index, label='Без индекса')
plt.title('Время вставки vs N')
plt.xlabel('N (количество записей)')
plt.ylabel('Время (с)')
plt.legend()

plt.subplot(3, 1, 2)
plt.plot(N_values, select_times_with_index, label='С индексом')
plt.plot(N_values, select_times_without_index, label='Без индекса')
plt.title('Время выборки vs N')
plt.xlabel('N (количество записей)')
plt.ylabel('Время (с)')
plt.legend()

plt.subplot(3, 1, 3)
plt.plot(N_values, delete_times_with_index, label='С индексом')
plt.plot(N_values, delete_times_without_index, label='Без индекса')
plt.title('Время удаления vs N')
plt.xlabel('N (количество записей)')
plt.ylabel('Время (с)')
plt.legend()

plt.tight_layout()
plt.show()

# удаляем созданные файлы
for table_name in ["table_a", "table_b", "table_with_index", "table_without_index"]:
    if os.path.exists(f"{table_name}.dat"):
        os.remove(f"{table_name}.dat")
    if os.path.exists(f"{table_name}.schema.json"):
        os.remove(f"{table_name}.schema.json")
    if os.path.exists(f"{table_name}_id.idx"):
        os.remove(f"{table_name}_id.idx")