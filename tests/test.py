import unittest
import os
import csv

import mysql.connector

from ..lib.workDB import create_tables, generate_data, insert_data, create_sandbox, delete_data, backup_table, restore_table, \
    measure_time, plot_graph, get_connection, TABLES


class TestLib(unittest.TestCase):
    def setUp(self):
        self.sandbox_db = "sandbox_db"
        try:
            create_sandbox(main_db="online_school", sandbox_db=self.sandbox_db)
            self.conn = mysql.connector.connect(
                host="localhost",
                user="qwsnxnjene",
                password="root",
                database=self.sandbox_db
            )
            self.cursor = self.conn.cursor()
            print("успешно установлено соединение с БД")
        except mysql.connector.Error as e:
            self.fail(f"ошибка при соединеннии с БД: {e}")

    # очищаем всё после каждого теста
    def tearDown(self):
        self.conn.close()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DROP DATABASE {self.sandbox_db}")

    def test_create_tables(self):
        """Тест успешного создания всех таблиц"""
        create_tables(self.sandbox_db)
        self.cursor.execute("SHOW TABLES")
        tables = [row[0] for row in self.cursor.fetchall()]
        expected_tables = list(TABLES.keys())
        for table in expected_tables:
            self.assertIn(table.lower(), tables, f"Таблица {table} не создана")

    def test_generate_data(self):
        """Тест заполнения всех таблиц"""
        create_tables(self.sandbox_db)
        data = generate_data("Вид_подготовки", 5)
        self.assertEqual(len(data), 5, "Кол-во сгенерированных строк не совпадает с запрошенным числом")
        for row in data:
            self.assertIsInstance(row[0], str, 'Неверная генерация данных')

        vid_data = generate_data("Вид_подготовки", 2)
        naprav_data = generate_data("Направление", 2)
        insert_data("Вид_подготовки", vid_data, self.sandbox_db)
        insert_data("Направление", naprav_data, self.sandbox_db)
        kurs_data = generate_data("Курс", 3)
        self.assertEqual(len(kurs_data), 3, "Неверная генерация данных")
        for row in kurs_data:
            self.assertIsInstance(row[0], int, "Неверная генерация данных")
            self.assertIsInstance(row[1], int, "Неверная генерация данных")

    def test_insert_data(self):
        """Тест вставки данных в таблицу"""
        create_tables(self.sandbox_db)
        data = generate_data("Вид_подготовки", 3)
        insert_data("Вид_подготовки", data, self.sandbox_db)
        self.cursor.execute("SELECT * FROM Вид_подготовки")
        rows = self.cursor.fetchall()
        self.assertEqual(len(rows), 3)
        for i, row in enumerate(rows):
            self.assertEqual(row[1], data[i][0], "Данные вставились неверно")

    def test_create_sandbox(self):
        """Тест создания песочницы"""
        self.cursor.execute("SHOW TABLES")
        sandbox_tables = [row[0] for row in self.cursor.fetchall()]
        expected_tables = list(TABLES.keys())
        for table in expected_tables:
            self.assertIn(table.lower(), sandbox_tables, f"Таблицы {table} нет в БД-песочнице")

    def test_delete_data(self):
        """Тест удаления данных из таблицы"""
        create_tables(self.sandbox_db)
        data = generate_data("Вид_подготовки", 3)
        insert_data("Вид_подготовки", data, self.sandbox_db)
        delete_data("Вид_подготовки", self.sandbox_db)
        self.cursor.execute("SELECT * FROM Вид_подготовки")
        rows = self.cursor.fetchall()
        self.assertEqual(len(rows), 0, "Таблица не была очищена")

    def test_backup_and_restore(self):
        """Тест возможности бэкапа и восстановления"""
        create_tables(self.sandbox_db)
        data = generate_data("Вид_подготовки", 3)
        insert_data("Вид_подготовки", data, self.sandbox_db)

        backup_file = "backup_test.csv"
        backup_table("Вид_подготовки", backup_file, self.sandbox_db)
        self.assertTrue(os.path.exists(backup_file), "Бэкап-файл не ыл создан")

        delete_data("Вид_подготовки", self.sandbox_db)
        restore_table("Вид_подготовки", backup_file, self.sandbox_db)
        self.cursor.execute("SELECT * FROM Вид_подготовки")
        rows = self.cursor.fetchall()
        self.assertEqual(len(rows), 3, "Данные восстановились неверно")

        os.remove(backup_file)

    def test_measure_time(self):
        """Тест измерения времени"""
        create_tables(self.sandbox_db)
        time = measure_time("SELECT * FROM Вид_подготовки", self.sandbox_db)
        self.assertGreater(time, 0, "Время(кол-во секунд) должно быть положительным")

    def test_plot_graph(self):
        """Тест на рисование графика"""
        x_data = [1, 2, 3]
        y_data_list = [[1, 2, 3], [4, 5, 6]]
        labels = ["Линия 1", "Линия 2"]
        title = "Тест графика"
        x_label = "X"
        y_label = "Y"
        save_path = "test_graph.png"

        plot_graph(x_data, y_data_list, labels, title, x_label, y_label, save_path)
        self.assertTrue(os.path.exists(save_path), "График не был создан")
        os.remove(save_path)


if __name__ == "__main__":
    unittest.main()