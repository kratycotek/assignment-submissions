import csv

from .base import (
    FeedContext,
    FileGate,
    GateAction,
    GateFileError,
    Layer,
    RawBatchGate,
    SourceStage,
    make_reject,
)


class HeaderGate(FileGate):
    code = "Q1"
    description = "File exists and its header matches the expected columns"
    fatal = True
    action = GateAction.FAIL

    def __init__(self, expected_columns: tuple[str, ...]):
        self._expected_columns = list(expected_columns)

    def check_file(self, ctx: FeedContext) -> None:
        with open(ctx.path, newline="") as handle:
            reader = csv.reader(handle)
            try:
                actual = next(reader)
            except StopIteration:
                raise GateFileError(self.code, "FILE_SCHEMA", value=[])
        if actual != self._expected_columns:
            raise GateFileError(self.code, "FILE_SCHEMA", value=actual)


class RowCountGate(RawBatchGate):
    code = "Q3"
    description = "A file has enough rows that survive the width check"
    fatal = True
    action = GateAction.FAIL

    def __init__(self, min_rows: int):
        self._min_rows = min_rows

    def split(self, rows, ctx: FeedContext):
        if len(rows) < self._min_rows:
            raise GateFileError(self.code, "ZERO_ROWS", value=len(rows))
        return list(rows), []


class RowWidthGate(RawBatchGate):
    code = "Q2"
    description = "Every data row has the same number of fields as the header"

    def split(self, rows, ctx: FeedContext):
        width = len(ctx.spec.columns)
        kept, rejects = [], []
        for fields in rows:
            if len(fields) == width:
                kept.append(list(fields))
            else:
                rejects.append(
                    make_reject(
                        ctx,
                        Layer.BRONZE,
                        SourceStage.FILE,
                        self.code,
                        "MALFORMED_ROW",
                        payload={"fields": list(fields)},
                    )
                )
        return kept, rejects
