import json
import struct
import re
import time


class Column:
    def __init__(self, name, type, length=None):
        self.name = name
        self.type = type
        self.length = length if type == 'VARCHAR' else 0


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.schema_file = f"{name}.schema.json"
        self.data_file = f"{name}.dat"

        for col in columns:
            if col.type == 'VARCHAR' and col.length is None:
                raise ValueError(f"Length not specified for VARCHAR column '{col.name}'")

        self.row_size = sum(8 if col.type == 'INT' else col.length for col in columns)

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
            raise ValueError("Количество значений не совпадает с количеством столбцов")

        row_data = b''
        for col, val in zip(self.columns, values):
            if col.type == 'INT':
                row_data += struct.pack('Q', val)
            elif col.type == 'VARCHAR':
                row_data += val.ljust(col.length).encode('utf-8')[:col.length]

        with open(self.data_file, 'ab') as f:
            offset = f.tell()
            f.write(row_data)
        for col, val in zip(self.columns, values):
            if col.type == 'INT':
                with open(self.index_files[col.name], 'ab') as idx_f:
                    idx_f.write(struct.pack('QQ', val, offset))

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
                        if idx_val == val:
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
                        if idx_val == val:
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
                        row = self._parse_row(row_data)
                        col_idx = self._get_column_index(col_name)
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
                val = struct.unpack('Q', row_data[pos:pos+8])[0]
                pos += 8
            elif col.type == 'VARCHAR':
                val = row_data[pos:pos+col.length].decode('utf-8').rstrip()
                pos += col.length
            row.append(val)
        return row

    def _get_column_index(self, col_name):
        return [c.name for c in self.columns].index(col_name)

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
            columns.append(Column(name, type, length))
        return table_name, columns
    raise ValueError("Неверный синтаксис CREATE TABLE")

def performance_test():
    sql = "CREATE TABLE test (id INT, data VARCHAR(50))"
    table_name, columns = parse_create_table(sql)
    table = Table(table_name, columns)

    start_time = time.time()
    for i in range(1000):
        table.insert([i, f"Data_{i}"])
    insert_time = time.time() - start_time
    print(f"Вставка 1000 строк: {insert_time:.4f} сек")

    start_time = time.time()
    result = table.select(where=('id', '=', 500))
    select_time = time.time() - start_time
    print(f"Выборка по индексу (id=500): {select_time:.4f} сек")

    start_time = time.time()
    table.delete(where=('id', '=', 500))
    delete_time = time.time() - start_time
    print(f"Удаление по индексу (id=500): {delete_time:.4f} сек")


if __name__ == "__main__":
    sql = "CREATE TABLE users (id INT, name VARCHAR(50))"
    table_name, columns = parse_create_table(sql)
    table = Table(table_name, columns)
    table.insert([1, 'Alice'])
    table.insert([2, 'Bob'])

    print("Все данные:", table.select())
    print("Имя с id=1:", table.select(columns=['name'], where=('id', '=', 1)))

    table.delete(where=('id', '=', 1))
    print("После удаления:", table.select())

    print("\nТестирование производительности:")
    performance_test()