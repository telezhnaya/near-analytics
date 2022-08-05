import abc
import psycopg2
import psycopg2.extras

from .base_aggregations import BaseAggregations
from .db_tables import time_json, daily_start_of_range


class SqlAggregations(BaseAggregations):
    @property
    @abc.abstractmethod
    def sql_create_table(self):
        pass

    @property
    @abc.abstractmethod
    def sql_drop_table(self):
        pass

    @property
    @abc.abstractmethod
    def sql_select(self):
        pass

    @property
    @abc.abstractmethod
    def sql_insert(self):
        pass

    def create_table(self):
        with self.analytics_connection.cursor() as analytics_cursor:
            try:
                analytics_cursor.execute(self.sql_create_table)
                self.analytics_connection.commit()
            except psycopg2.errors.DuplicateTable:
                self.analytics_connection.rollback()

    def drop_table(self):
        with self.analytics_connection.cursor() as analytics_cursor:
            try:
                analytics_cursor.execute(self.sql_drop_table)
                self.analytics_connection.commit()
            except psycopg2.errors.UndefinedTable:
                self.analytics_connection.rollback()

    def collect(self, requested_timestamp: int) -> list:
        with self.indexer_connection.cursor() as indexer_cursor:
            indexer_cursor.execute(
                self.sql_select, time_json(daily_start_of_range(requested_timestamp))
            )
            result = indexer_cursor.fetchall()
            return self.prepare_data(result)

    def store(self, parameters: list):
        chunk_size = 100
        with self.analytics_connection.cursor() as analytics_cursor:
            for i in range(0, len(parameters), chunk_size):
                try:
                    psycopg2.extras.execute_values(
                        analytics_cursor,
                        self.sql_insert,
                        parameters[i : i + chunk_size],
                    )
                    self.analytics_connection.commit()
                except psycopg2.errors.UniqueViolation:
                    self.analytics_connection.rollback()

    # Overload this method if you need to prepare data before insert
    @staticmethod
    def prepare_data(parameters, **kwargs) -> list:
        return parameters
