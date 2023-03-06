from typing import Any

from flask_admin.babel import lazy_gettext
from flask_admin.contrib.sqla.filters import BaseSQLAFilter
from sqlalchemy import func
from sqlalchemy.orm.query import Query


class StringInArrayColumn(BaseSQLAFilter):
    def apply(self, query: Query, value: str, _: Any = None) -> Query:  # noqa: ANN401
        return query.filter(func.array_to_string(self.column, ", ").ilike(f"%{value}%"))

    def operation(self) -> None:
        return lazy_gettext("contains")


class StringNotInArrayColumn(BaseSQLAFilter):
    def apply(self, query: Query, value: str, _: Any = None) -> Query:  # noqa: ANN401
        return query.filter(func.array_to_string(self.column, ", ").notilike(f"%{value}%"))

    def operation(self) -> None:
        return lazy_gettext("not contains")
