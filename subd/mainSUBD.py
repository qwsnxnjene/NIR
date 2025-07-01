import json
import struct
import re
import time


class Column:
    def __init__(self, name, type):
        self.name = name
        self.type = type
        self.length = len(type) if 'VARCHAR' in type else 0


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.schema_file = f"{name}.schema.json"
        self.data_file = f"{name}.dat"
        # определяем размер строки, 1 байт для числовых данных, 2 байта на символ для строки
        self.row_size = sum(8 if col.type == 'INT' else col.length * 2 for col in columns)
        schema = {
            'columns': [{'name': col.name, 'type': col.type, 'length': col.length} for col in columns]
        }
        with open(self.schema_file, 'w') as f:
            json.dump(schema, f)

        open(self.data_file, 'wb').close()
        self.index_files = {col.name: f"{name}_{col.name}.idx" for col in columns if col.type == 'INT'}

        for col in columns:
            if col.type == 'INT':
                open(self.index_files[col.name], 'wb').close()

    def insert(self, values):
        if len(values) != len(self.columns):
            raise ValueError("количество значений не совпадает с количеством столбцов")

        row_data = b''
        for col, val in zip(self.columns, values):
            if col.type == 'INT':
                row_data += struct.pack('Q', int(val))
            elif 'VARCHAR' in col.type:
                val_str = str(val)
                encoded = val_str.encode('utf-16')
                if len(encoded) % 2 != 0:
                    encoded = encoded[:-1]
                row_data += encoded.ljust(col.length * 2, b'\0')

        with open(self.data_file, 'ab') as f:
            offset = f.tell()
            f.write(row_data)
        for col, val in zip(self.columns, values):
            if col.type == 'INT':
                with open(self.index_files[col.name], 'ab') as idx_f:
                    idx_f.write(struct.pack('QQ', int(val), offset))

    def select(self, columns='*', where=None):
        results = []
        if where and where[0] in self.index_files:
            col_name, op, val = where
            if op == '=':
                with open(self.index_files[col_name], 'rb') as idx_f:
                    while True:
                        index_data = idx_f.read(16)
                        if not index_data:
                            break
                        idx_val, offset = struct.unpack('QQ', index_data)
                        if idx_val == int(val):
                            with open(self.data_file, 'rb') as f:
                                f.seek(offset)
                                row_data = f.read(self.row_size)
                                row = self._parse_row(row_data)
                                if columns == '*':
                                    results.append(row)
                                else:
                                    col_indices = [self._get_column_index(col) for col in columns]
                                    results.append([row[i] for i in col_indices])
        else:
            with open(self.data_file, 'rb') as f:
                offset = 0
                while True:
                    f.seek(offset)
                    row_data = f.read(self.row_size)
                    if not row_data:
                        break
                    row = self._parse_row(row_data)
                    if where:
                        col_name, op, val = where
                        col_idx = self._get_column_index(col_name)
                        col_type = self.columns[col_idx].type
                        if col_type == 'INT':
                            val = int(val)
                        if op == '=' and row[col_idx] != val:
                            offset += self.row_size
                            continue
                    if columns == '*':
                        results.append(row)
                    else:
                        col_indices = [self._get_column_index(col) for col in columns]
                        results.append([row[i] for i in col_indices])
                    offset += self.row_size
        return results

    def delete(self, where=None):
        if where:
            col_name, op, val = where
            if col_name in self.index_files and op == '=':
                with open(self.index_files[col_name], 'rb') as idx_f:
                    offsets_to_delete = []
                    while True:
                        index_data = idx_f.read(16)
                        if not index_data:
                            break
                        idx_val, offset = struct.unpack('QQ', index_data)
                        if idx_val == int(val):
                            offsets_to_delete.append(offset)
                with open(self.data_file, 'rb') as f:
                    new_data = b''
                    offset = 0
                    while True:
                        f.seek(offset)
                        row_data = f.read(self.row_size)
                        if not row_data:
                            break
                        if offset not in offsets_to_delete:
                            new_data += row_data
                        offset += self.row_size
                with open(self.data_file, 'wb') as f:
                    f.write(new_data)
                for idx_file in self.index_files.values():
                    open(idx_file, 'wb').close()
                with open(self.data_file, 'rb') as f:
                    offset = 0
                    while True:
                        f.seek(offset)
                        row_data = f.read(self.row_size)
                        if not row_data:
                            break
                        row = self._parse_row(row_data)
                        for col, val in zip(self.columns, row):
                            if col.type == 'INT':
                                with open(self.index_files[col.name], 'ab') as idx_f:
                                    idx_f.write(struct.pack('QQ', val, offset))
                        offset += self.row_size
            else:
                with open(self.data_file, 'rb') as f:
                    new_data = b''
                    offset = 0
                    while True:
                        f.seek(offset)
                        row_data = f.read(self.row_size)
                        if not row_data:
                            break
                        if len(row_data) != self.row_size:
                            print(f"Предупреждение: Неполная строка на смещении {offset}, пропускается")
                            offset += self.row_size
                            continue
                        row = self._parse_row(row_data)
                        col_idx = self._get_column_index(col_name)
                        col_type = self.columns[col_idx].type
                        if col_type == 'INT':
                            val = int(val)
                        if not (op == '=' and row[col_idx] == val):
                            new_data += row_data
                        offset += self.row_size
                with open(self.data_file, 'wb') as f:
                    f.write(new_data)
                for idx_file in self.index_files.values():
                    open(idx_file, 'wb').close()
                with open(self.data_file, 'rb') as f:
                    offset = 0
                    while True:
                        f.seek(offset)
                        row_data = f.read(self.row_size)
                        if not row_data:
                            break
                        row = self._parse_row(row_data)
                        for col, val in zip(self.columns, row):
                            if col.type == 'INT':
                                with open(self.index_files[col.name], 'ab') as idx_f:
                                    idx_f.write(struct.pack('QQ', val, offset))
                        offset += self.row_size
        else:
            open(self.data_file, 'wb').close()
            for idx_file in self.index_files.values():
                open(idx_file, 'wb').close()

    def _parse_row(self, row_data):
        row = []
        pos = 0
        for col in self.columns:
            if col.type == 'INT':
                val = struct.unpack('Q', row_data[pos:pos + 8])[0]
                pos += 8
            elif 'VARCHAR' in col.type:
                try:
                    val_bytes = row_data[pos:pos + col.length * 2]
                    val = val_bytes.decode('utf-16').rstrip('\0')
                except UnicodeDecodeError as e:
                    print(f"[Table._parse_row]: ошибка декодирования: {e}")
                    val = ""
                pos += col.length * 2
            row.append(val)
        return row

    def _get_column_index(self, col_name):
        return [c.name for c in self.columns].index(col_name)


class Database:
    def __init__(self):
        self.tables = {}

    def execute(self, sql):
        sql = sql.strip()
        if sql.startswith('CREATE TABLE'):
            table_name, columns = parse_create_table(sql)
            if table_name in self.tables:
                raise ValueError(f"Таблица '{table_name}' уже существует")
            self.tables[table_name] = Table(table_name, columns)
        elif sql.startswith('INSERT INTO'):
            table_name, values = parse_insert(sql)
            if table_name not in self.tables:
                raise ValueError(f"Таблица '{table_name}' не существует")
            self.tables[table_name].insert(values)
        elif sql.startswith('SELECT'):
            table_name, columns, where = parse_select(sql)
            if table_name not in self.tables:
                raise ValueError(f"Таблица '{table_name}' не существует")
            return self.tables[table_name].select(columns, where)
        elif sql.startswith('DELETE FROM'):
            table_name, where = parse_delete(sql)
            if table_name not in self.tables:
                raise ValueError(f"Таблица '{table_name}' не существует")
            self.tables[table_name].delete(where)
        else:
            raise ValueError("Неизвестный SQL-запрос")


def parse_create_table(sql):
    match = re.match(r'CREATE TABLE (\w+) \((.+)\)', sql)
    if match:
        table_name = match.group(1)
        columns_str = match.group(2)
        columns = []
        for col_def in columns_str.split(','):
            parts = col_def.strip().split()
            name = parts[0]
            type = parts[1]
            length = int(parts[2][1:-1]) if type == 'VARCHAR' else None
            columns.append(Column(name, type))
        return table_name, columns
    raise ValueError("неверный синтаксис CREATE TABLE")


def parse_select(sql):
    pattern = r'SELECT (.+) FROM (\w+)( WHERE (.+))?'
    match = re.match(pattern, sql)
    if match:
        columns_str = match.group(1)
        table_name = match.group(2)
        where_str = match.group(4)

        if columns_str.strip() == '*':
            columns = '*'
        else:
            columns = [col.strip() for col in columns_str.split(',')]

        if where_str:
            where_match = re.match(r'(\w+) = (.+)', where_str)
            if where_match:
                col_name = where_match.group(1)
                value = where_match.group(2).strip("'\"")
                where = (col_name, '=', value)
            else:
                raise ValueError("неверный синтаксис оператора WHERE")
        else:
            where = None

        return table_name, columns, where
    raise ValueError("неверный синтаксис SELECT")


def parse_insert(sql):
    pattern = r'INSERT INTO (\w+) VALUES \((.+)\)'
    match = re.match(pattern, sql)
    if match:
        table_name = match.group(1)
        values_str = match.group(2)
        values = [val.strip().strip("'\"") for val in values_str.split(',')]
        return table_name, values
    raise ValueError("неверный синтаксис INSERT")


def parse_delete(sql):
    pattern = r'DELETE FROM (\w+)( WHERE (.+))?'
    match = re.match(pattern, sql)
    if match:
        table_name = match.group(1)
        where_str = match.group(3)

        if where_str:
            where_match = re.match(r'(\w+) = (.+)', where_str)
            if where_match:
                col_name = where_match.group(1)
                value = where_match.group(2).strip("'\"")
                where = (col_name, '=', value)
            else:
                raise ValueError("неверный синтаксис WHERE")
        else:
            where = None

        return table_name, where
    raise ValueError("yеверный синтаксис DELETE")


def performance_test():
    db = Database()
    db.execute("CREATE TABLE test (id INT, data VARCHAR(50))")

    start_time = time.time()
    for i in range(1000):
        db.execute(f"INSERT INTO test VALUES ({i}, 'Data_{i}')")
    insert_time = time.time() - start_time
    print(f"вставка 1000 строк: {insert_time:.4f} сек")

    start_time = time.time()
    result = db.execute("SELECT * FROM test WHERE id = 500")
    select_time = time.time() - start_time
    print(f"выборка по индексу (id=500): {select_time:.4f} сек")

    start_time = time.time()
    db.execute("DELETE FROM test WHERE id = 500")
    delete_time = time.time() - start_time
    print(f"удаление по индексу (id=500): {delete_time:.4f} сек")


if __name__ == "__main__":
    db = Database()

    db.execute("CREATE TABLE users (id INT, name VARCHAR(50))")

    db.execute("INSERT INTO users VALUES (1, \'Alice\')")
    db.execute("INSERT INTO users VALUES (2, \'Bob\')")
    db.execute("INSERT INTO users VALUES (3, \'曹\')")

    print("Все данные:", db.execute("SELECT * FROM users"))
    print(db.execute("SELECT name FROM users"))

    print("Имя с id=1:", db.execute("SELECT name FROM users WHERE id = 1"))
    print("Имя с id=3:", db.execute("SELECT name FROM users WHERE id = 3"))

    db.execute("DELETE FROM users WHERE id = 1")
    print("После удаления поля с id=1:", db.execute("SELECT * FROM users"))
    print()
    print("тестируем производительность:")
    performance_test()