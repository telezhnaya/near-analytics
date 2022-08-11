import abc
import dataclasses
import psycopg2


# Base class with all public methods needed to interact with each aggregation
@dataclasses.dataclass
class BaseAggregations(abc.ABC):
    analytics_connection: psycopg2.extensions.connection
    indexer_connection: psycopg2.extensions.connection

    # Collects the aggregations for the requested_timestamp.
    # If it's not possible to compute aggregations for given requested_timestamp,
    # (for example, we ask about daily stats for today: the day is not ended),
    # empty list will be returned.
    # The method should return the list of values,
    # each of the values should be possible to add to the table (from `create_table` method) without any changes
    @abc.abstractmethod
    def collect(self, requested_timestamp: int) -> list:
        pass

    @abc.abstractmethod
    def store(self, parameters: list):
        pass

    @abc.abstractmethod
    def create_table(self):
        pass

    @abc.abstractmethod
    def drop_table(self):
        pass


# Be careful, don't create circular dependencies
BaseAggregations.DEPENDENCIES = []
