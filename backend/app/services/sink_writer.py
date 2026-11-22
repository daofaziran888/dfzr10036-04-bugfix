"""结果落盘服务：写入完成后触发 iceberg_commit 原子提交和 data_diff 比对。

落盘归属规则：
- 落盘管道按「源端」授权，只有源端编号匹配的管道才具备本源落盘权限；
  仅目标表或同名数据流相同的他源管道属于越权写入，不得选用。
- 管道还需处于「写入中」且当前落在「有效期」内才可用；
  同一源端多条管道有效期交接时，由最新生效的授权管道接管，与列表顺序无关。
- 每个已受理解析批次只落盘一次：批次未确认前可断点续做，已确认后重发不再重复提交。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

_VALIDITY_FMT = "%Y-%m-%d %H:%M:%S"


class SinkWriterService(BaseModuleService):
    MODULE = "sink_writer"
    ENTITY = "落盘管道"
    OPERATION_TERM = "写入"
    LIST_FIELDS = ["管道编号", "源端", "数据流", "目标载体", "写入模式", "分区列", "批次大小", "压缩格式", "写入速率", "有效期"]
    REQUIRED_FIELDS = ["管道编号", "数据流", "目标载体"]
    STATUS_ORDER = ["草稿", "写入中", "写入阻塞", "已停止"]
    ACTION_RULES = {"启动写入": "写入中", "解除阻塞": "写入阻塞", "停止写入": "已停止"}
    NEGATIVE_ACTIONS = []

    # ---- 本源落盘权限 ----

    @staticmethod
    def _validity_window(row: dict[str, Any]) -> tuple[datetime, datetime] | None:
        raw = str(row.get("有效期", ""))
        if "~" not in raw:
            return None
        start_raw, end_raw = raw.split("~", 1)
        try:
            start = datetime.strptime(start_raw.strip(), _VALIDITY_FMT)
            end = datetime.strptime(end_raw.strip(), _VALIDITY_FMT)
        except ValueError:
            return None
        return start, end

    def authorized_pipelines(self, source_id: str) -> list[dict[str, Any]]:
        """源端编号匹配的授权落盘管道（不论状态/有效期）。"""
        return [
            row for row in store.rows(self.MODULE)
            if str(row.get("源端", "")) == str(source_id)
        ]

    def usable_pipelines(self, source_id: str, now: datetime) -> list[dict[str, Any]]:
        """授权管道中当前可投入写入的：写入中且处于有效期内。"""
        usable: list[dict[str, Any]] = []
        for row in self.authorized_pipelines(source_id):
            if row.get("status") != "写入中":
                continue
            window = self._validity_window(row)
            if window is None or not (window[0] <= now < window[1]):
                continue
            usable.append(row)
        return usable

    def resolve_write_pipeline(self, source_id: str, now: datetime) -> dict[str, Any] | None:
        """生成快照前确认本次源端唯一接管的落盘管道。

        有效期交接（如 SK-A1 → SK-A2）时，最新生效的授权管道接管；
        排序只依赖有效期与管道编号，与管道列表顺序无关。
        """
        usable = self.usable_pipelines(source_id, now)
        if not usable:
            return None

        def sort_key(row: dict[str, Any]) -> tuple[datetime, str]:
            window = self._validity_window(row)
            return (window[0] if window else datetime.min, str(row.get("管道编号", "")))

        return max(usable, key=sort_key)

    def unauthorized_lookalikes(self, source_id: str) -> list[dict[str, Any]]:
        """只匹配目标表或同名数据流、却无本源授权的他源管道（越权写入，拦截用）。"""
        authorized = self.authorized_pipelines(source_id)
        targets = {str(row.get("目标载体", "")) for row in authorized}
        streams = {str(row.get("数据流", "")) for row in authorized}
        lookalikes: list[dict[str, Any]] = []
        for row in store.rows(self.MODULE):
            if str(row.get("源端", "")) == str(source_id):
                continue
            if str(row.get("目标载体", "")) in targets or str(row.get("数据流", "")) in streams:
                lookalikes.append(row)
        return lookalikes

    # ---- 已受理批次 ----

    def accept_batch(
        self,
        source_id: str,
        parse_rule: dict[str, Any],
        batch_no: str | None = None,
    ) -> dict[str, Any] | None:
        """受理/沿用解析批次：把解析规则和字段映射绑定到批次上。

        - 指定 batch_no：沿用该已受理批次（补齐绑定或中断续做），不重复受理；
        - 未指定：源端存在解析就绪的未确认批次则沿用，否则受理新批次。
        """
        if batch_no:
            record = store.handoff_batch(batch_no)
            if record is None or record.get("源端") != str(source_id):
                return None
            return record

        open_record = store.open_handoff_batch(str(source_id))
        if open_record is not None:
            return open_record

        seq = len(store.handoff_batches_for(str(source_id))) + 1
        record = {
            "批次号": f"BATCH-{source_id}-{seq:04d}",
            "源端": str(source_id),
            "解析规则": str(parse_rule.get("规则编号", "")),
            "字段映射": str(parse_rule.get("字段映射", "")),
            "状态": "解析就绪",
            "管道编号": None,
            "快照编号": None,
            "受理时间": datetime.now().strftime(_VALIDITY_FMT),
            "确认时间": None,
        }
        store.save_handoff_batch(record)
        return record

    def commit_batch(
        self,
        batch: dict[str, Any],
        pipeline: dict[str, Any],
    ) -> dict[str, Any] | None:
        """把已受理批次交给授权管道落盘；同一解析批次只落一次。"""
        if batch.get("状态") == "落盘确认":
            return None
        commit = self.trigger_iceberg_commit(int(pipeline["id"]), batch=batch)
        if commit is None:
            return None
        batch["状态"] = "落盘确认"
        batch["管道编号"] = str(pipeline.get("管道编号", ""))
        batch["快照编号"] = str(commit.get("快照编号", ""))
        batch["确认时间"] = datetime.now().strftime(_VALIDITY_FMT)
        return commit

    def trigger_iceberg_commit(
        self,
        entry_id: int,
        batch: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """落盘管道写入中时，触发 iceberg_commit 原子提交快照。

        数据流链路: sink_writer -> iceberg_commit
        快照归属以管道为准：目标表取自管道目标载体，数据文件路径携带管道编号；
        带批次调用时同一批次号只生成一条快照。
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入中":
            return None

        batch_no = str(batch.get("批次号", "")) if batch else ""
        commit_rows = store.rows("iceberg_commit")
        if batch_no:
            for row in commit_rows:
                if str(row.get("批次号", "")) == batch_no:
                    return None  # 该批次已生成过快照，拒绝重复提交

        pipeline_no = str(entry.get("管道编号", "default"))
        new_id = max((int(r.get("id", 0)) for r in commit_rows), default=0) + 1
        commit = {
            "id": new_id,
            "快照编号": f"ICEB-{new_id:04d}",
            "管道编号": pipeline_no,
            "批次号": batch_no,
            "目标表": str(entry.get("目标载体", "unknown")),
            "数据文件": f"s3://warehouse/data/{pipeline_no}/part-{new_id:05d}.parquet",
            "清单文件": f"metadata/manifest-{new_id}.avro",
            "快照ID": f"{new_id * 1000 + 500}",
            "提交状态": "待提交",
            "影响行数": 128340,
            "提交时间": "2026-11-18 10:32:45",
            "status": "提交中",
            "pending": True,
            "abnormal": False,
        }
        commit_rows.append(commit)
        return commit

    def trigger_block_cleanup(self, entry_id: int) -> dict[str, Any] | None:
        """写入阻塞时触发 compaction 合并小文件。

        数据流链路: sink_writer(写入阻塞) -> compaction
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "写入阻塞":
            return None

        comp_rows = store.rows("compaction")
        new_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
        comp = {
            "id": new_id,
            "任务编号": f"COMP-{new_id:04d}",
            "目标表": str(entry.get("目标载体", "unknown")),
            "文件数量": 85,
            "合并策略": "BinPack-按大小合并",
            "触发阈值": "文件数 >= 50",
            "执行内存": "2GB",
            "合并耗时": "未执行",
            "释放空间": "未执行",
            "status": "合并中",
            "pending": True,
            "abnormal": False,
        }
        comp_rows.append(comp)
        return comp
