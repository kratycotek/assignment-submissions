"""Table-name registry. dags/medallion/tables.yaml is the source of truth."""

from pathlib import Path

import yaml


class Tables:
    def __init__(self, mapping: dict):
        self._raw = mapping
        for role, names in mapping.items():
            for key, value in names.items():
                setattr(self, f"{role}_{key}", value)
        self.control_batch_log = mapping["control"]["batch_log"]
        self.control_param = mapping["control"]["param"]
        self.control_rej_table = mapping["control"]["rej_table"]
        self.control_alert_table = mapping["control"]["alert_table"]
        self.control_load_dates = mapping["control"]["load_dates"]
        self.control_batch_scripts = mapping["control"]["batch_scripts"]

    @classmethod
    def load(cls, path: Path) -> "Tables":
        return cls(yaml.safe_load(Path(path).read_text()))


def load_tables(path: Path) -> Tables:
    return Tables.load(path)
