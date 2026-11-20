import abc
from collections.abc import Iterable, Sequence
from typing import Any


class Warehouse(abc.ABC):
    param: str
    json_param: str

    @abc.abstractmethod
    def execute(self, sql: str, params: Sequence[Any] = ()) -> None: ...

    @abc.abstractmethod
    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None: ...

    @abc.abstractmethod
    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]: ...

    @abc.abstractmethod
    def close(self) -> None: ...

    def insert_rows(
        self,
        table: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
    ) -> int:
        rows = list(rows)
        if not rows:
            return 0
        placeholders = ", ".join([self.param] * len(columns))
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        self.executemany(sql, rows)
        return len(rows)


class SnowflakeWarehouse(Warehouse):
    param = "%s"
    json_param = "PARSE_JSON(%s)"

    def __init__(self, connection: Any, tables=None):
        self._connection = connection
        self.tables = tables

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, tuple(params))
        finally:
            cursor.close()
        self._connection.commit()

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        cursor = self._connection.cursor()
        try:
            for row in rows:
                cursor.execute(sql, tuple(row))
        finally:
            cursor.close()
        self._connection.commit()

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql, tuple(params))
            return cursor.fetchall()
        finally:
            cursor.close()

    def close(self) -> None:
        self._connection.close()


class DuckDbWarehouse(Warehouse):
    param = "?"
    json_param = "CAST(? AS JSON)"

    def __init__(self, connection: Any, tables=None):
        self._connection = connection
        self.tables = tables

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self._connection.execute(sql, tuple(params))

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        rows = [tuple(r) for r in rows]
        if rows:
            self._connection.executemany(sql, rows)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
        return self._connection.execute(sql, tuple(params)).fetchall()

    def close(self) -> None:
        self._connection.close()
