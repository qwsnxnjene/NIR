import mysql.connector
from timeit import timeit
import matplotlib.pyplot as plt

db = mysql.connector.connect(
    host="localhost",
    user="qwsnxnjene",
    password="root",
    database="online_school"
)
cursor = db.cursor()


cursor.execute("""
    CREATE TABLE IF NOT EXISTS Курс (
        id INT PRIMARY KEY,
        name VARCHAR(100)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Курс_без_PK (
        id INT,
        name VARCHAR(100)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Наставник (
        id INT PRIMARY KEY AUTO_INCREMENT,
        fio VARCHAR(100),
        INDEX idx_fio (fio)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Наставник_без_индекса (
        id INT PRIMARY KEY AUTO_INCREMENT,
        fio VARCHAR(100)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Описания_курсов (
        id INT PRIMARY KEY AUTO_INCREMENT,
        description TEXT,
        FULLTEXT (description)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS Описания_курсов_без_индекса (
        id INT PRIMARY KEY AUTO_INCREMENT,
        description TEXT
    )
""")

tables = ["Курс", "Курс_без_PK", "Наставник", "Наставник_без_индекса", "Описания_курсов", "Описания_курсов_без_индекса"]
for table in tables:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute(f"TRUNCATE TABLE {table}")

for i in range(1, 10001):
    cursor.execute("INSERT INTO Курс (id, name) VALUES (%s, %s)", (i, f"Курс_{i}"))
    cursor.execute("INSERT INTO Курс_без_PK (id, name) VALUES (%s, %s)", (i, f"Курс_{i}"))

    cursor.execute("INSERT INTO Наставник (fio) VALUES (%s)", (f"Наставник_{i}",))
    cursor.execute("INSERT INTO Наставник_без_индекса (fio) VALUES (%s)", (f"Наставник_{i}",))

    description = f"Описание курса {i} с Python и SQL"
    cursor.execute("INSERT INTO Описания_курсов (description) VALUES (%s)", (description,))
    cursor.execute("INSERT INTO Описания_курсов_без_индекса (description) VALUES (%s)", (description,))

db.commit()

def query_kurs_with_pk():
    cursor.execute("SELECT * FROM Курс WHERE id = 5000")
    return cursor.fetchall()


def query_kurs_without_pk():
    cursor.execute("SELECT * FROM Курс_без_PK WHERE id = 5000")
    return cursor.fetchall()


def query_nastavnik_with_index():
    cursor.execute("SELECT * FROM Наставник WHERE fio = 'Наставник_5000'")
    return cursor.fetchall()


def query_nastavnik_without_index():
    cursor.execute("SELECT * FROM Наставник_без_индекса WHERE fio = 'Наставник_5000'")
    return cursor.fetchall()


def query_opis_with_fulltext():
    cursor.execute("SELECT * FROM Описания_курсов WHERE MATCH(description) AGAINST('Python')")
    return cursor.fetchall()


def query_opis_without_fulltext():
    cursor.execute("SELECT * FROM Описания_курсов_без_индекса WHERE description LIKE '%Python%'")
    return cursor.fetchall()


time_kurs_with_pk = timeit(query_kurs_with_pk, number=100)
time_kurs_without_pk = timeit(query_kurs_without_pk, number=100)

time_nastavnik_with_index = timeit(query_nastavnik_with_index, number=100)
time_nastavnik_without_index = timeit(query_nastavnik_without_index, number=100)

time_opis_with_fulltext = timeit(query_opis_with_fulltext, number=100)
time_opis_without_fulltext = timeit(query_opis_without_fulltext, number=100)

print(f"Время для Курс с PK: {time_kurs_with_pk:.6f} сек")
print(f"Время для Курс без PK: {time_kurs_without_pk:.6f} сек")
print(f"Время для Наставник с индексом: {time_nastavnik_with_index:.6f} сек")
print(f"Время для Наставник без индекса: {time_nastavnik_without_index:.6f} сек")
print(f"Время для Описания_курсов с FULLTEXT: {time_opis_with_fulltext:.6f} сек")
print(f"Время для Описания_курсов без FULLTEXT: {time_opis_without_fulltext:.6f} сек")

labels = ['Курс с PK', 'Курс без PK', 'Наставник с индексом', 'Наставник без индекса', 'Описания с FULLTEXT',
          'Описания без FULLTEXT']
times = [time_kurs_with_pk, time_kurs_without_pk, time_nastavnik_with_index, time_nastavnik_without_index,
         time_opis_with_fulltext, time_opis_without_fulltext]

plt.bar(labels, times)
plt.title('Сравнение времени выполнения запросов')
plt.ylabel('Время (сек)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

db.close()