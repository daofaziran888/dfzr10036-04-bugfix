"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 落盘数据交接台账：批次号 -> 已受理批次（解析就绪/落盘确认）。
        # 独立于业务模块表，保证同一解析批次跨管道交接只落一次、可断点续做。
        self._handoff_batches: dict[str, dict[str, Any]] = {}

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    # ---- 落盘数据交接批次台账 ----

    def handoff_batch(self, batch_no: str) -> dict[str, Any] | None:
        return self._handoff_batches.get(batch_no)

    def save_handoff_batch(self, record: dict[str, Any]) -> None:
        self._handoff_batches[str(record["批次号"])] = record

    def handoff_batches_for(self, source_id: str) -> list[dict[str, Any]]:
        return [
            record for record in self._handoff_batches.values()
            if record.get("源端") == source_id
        ]

    def open_handoff_batch(self, source_id: str) -> dict[str, Any] | None:
        """该源端最近一个已受理但尚未落盘确认的批次（解析就绪待交接）。"""
        for record in reversed(list(self._handoff_batches.values())):
            if record.get("源端") == source_id and record.get("状态") == "解析就绪":
                return record
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
