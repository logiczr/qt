"""silver.f10_doc F10 文档构建器。

Bronze 来源：
  - bronze.raw_tdx_f10（元数据指针，指向 data/bronze/f10/ 下的 txt 文件）

处理逻辑：
  - 读取 Bronze txt 文件 → 解析制表符表格 + 文本段落 → 渲染 HTML
  - HTML 文件落盘 data/silver/f10/{code}_{YYYYMMDD}.html
  - DB 表存元数据指针（file_path / file_hash / source_hash）

Silver 目标：
  stock_code       VARCHAR   PK
  trade_date       DATE      PK  (从文件名提取的更新日期)
  file_path        VARCHAR       HTML 文件路径
  file_size        BIGINT        文件大小(bytes)
  file_hash        VARCHAR       HTML 文件 SHA256 前16位
  source_hash      VARCHAR       Bronze 源 txt 的 file_hash（去重依据）
  ingested_at      TIMESTAMP     构建时间
  source_system    VARCHAR       'tdx'
  batch_id         VARCHAR       批次ID
"""

import hashlib
import logging
import os
import re
import time
from datetime import date, datetime

import pandas as pd
from db import get_db
from silver.base import BaseBuilder, BuildOrder, BuildResult, QualityIssue

_log = logging.getLogger(__name__)

# ---- HTML 模板 ----

_CSS = """\
body { font-family: "Microsoft YaHei", "PingFang SC", sans-serif; margin: 2em; background: #fafafa; color: #333; }
nav { position: sticky; top: 0; background: #fff; border-bottom: 1px solid #ddd; padding: 8px 0; z-index: 10; }
nav a { margin-right: 12px; text-decoration: none; color: #1a73e8; font-size: 14px; }
nav a:hover { text-decoration: underline; }
section { margin-bottom: 2em; }
h1 { font-size: 1.3em; color: #1a1a1a; border-bottom: 2px solid #1a73e8; padding-bottom: 4px; }
h2 { font-size: 1.1em; color: #444; margin-top: 1.2em; }
table { border-collapse: collapse; margin: 0.6em 0; font-size: 13px; background: #fff; }
th, td { border: 1px solid #d0d0d0; padding: 4px 10px; text-align: left; white-space: nowrap; }
th { background: #e8f0fe; font-weight: 600; }
td { background: #fff; }
td.empty { color: #bbb; }
.announcement { margin: 0.8em 0; padding: 0.6em 1em; border-left: 3px solid #1a73e8; background: #fff; }
.announcement .date { font-weight: 600; color: #1a73e8; }
.announcement .title { margin-left: 0.5em; }
.announcement .body { margin-top: 0.4em; white-space: pre-wrap; font-size: 13px; color: #555; }
p.content { white-space: pre-wrap; margin: 0.4em 0; line-height: 1.6; }
a.ext { color: #1a73e8; word-break: break-all; }
pre.dash-table { background: #fff; border: 1px solid #d0d0d0; padding: 8px 12px; font-size: 13px; line-height: 1.4; overflow-x: auto; white-space: pre; font-family: "Cascadia Code", "Consolas", monospace; }
.disclaimer { color: #999; font-size: 12px; margin-top: 2em; border-top: 1px solid #eee; padding-top: 0.8em; }
"""

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>{code} {name} F10</title>
<style>{css}</style>
</head>
<body>
{nav}
{body}
</body>
</html>
"""


def _linkify(text: str) -> str:
    """将文本中的 URL 转为 HTML 超链接。"""
    return re.sub(
        r'(https?://\S+)',
        r'<a class="ext" href="\1" target="_blank">\1</a>',
        text,
    )


def _esc(text: str) -> str:
    """HTML 转义。"""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _cell_value(raw: str) -> str:
    """处理单元格值：清理空白，--- → empty class。"""
    val = raw.strip()
    if val == "---" or val == "":
        return '<td class="empty">-</td>'
    return f"<td>{_esc(val)}</td>"


def _header_value(raw: str) -> str:
    """处理表头值。"""
    val = raw.strip()
    # 去掉前缀标记如 ●
    val = val.lstrip("●★☆◆◇■□")
    return f"<th>{_esc(val)}</th>"


class F10DocBuilder(BaseBuilder):
    target = "f10_doc"

    # ---- TXT → HTML 核心解析 -----------------------------------------------

    @staticmethod
    def _split_sections(content: str) -> list[tuple[str, str]]:
        """按 === xxx === 切板块，返回 [(section_name, section_text), ...]。"""
        parts = re.split(r'^=== (.+?) ===$', content, flags=re.MULTILINE)
        # parts: [前导文本, name1, text1, name2, text2, ...]
        sections = []
        for i in range(1, len(parts) - 1, 2):
            name = parts[i].strip()
            text = parts[i + 1]
            if name and text.strip():
                sections.append((name, text))
        return sections

    @staticmethod
    def _extract_stock_info(section_text: str) -> str:
        """从板块首行提取股票名称，如 '最新提示☆ ◇000001 平安银行 更新日期：...'。"""
        m = re.search(r'◇\d+\s+(\S+)', section_text)
        return m.group(1) if m else ""

    @staticmethod
    def _extract_update_date(section_text: str) -> str:
        """提取更新日期，如 '更新日期：2026-06-21'。"""
        m = re.search(r'更新日期[：:]\s*(\d{4}-\d{2}-\d{2})', section_text)
        return m.group(1) if m else ""

    @classmethod
    def _parse_table_block(cls, lines: list[str]) -> str:
        """将 box-drawing 字符表格转为 HTML <table>。"""
        if not lines:
            return ""

        # 收集数据行（跳过 ┌ └ ├ ┼ ┤ 等分隔线）
        data_rows = []
        is_header = True

        for line in lines:
            stripped = line.strip()
            # 分隔线跳过，但 ├───┼───┤ 分隔线标记 thead/tbody 边界
            if stripped.startswith("┌") or stripped.startswith("└"):
                if stripped.startswith("└"):
                    is_header = False  # 第一个 └ 之后的数据行进入 tbody
                continue
            if stripped.startswith("├"):
                is_header = False
                continue

            # 数据行：以 │ 开始
            if "│" in stripped:
                cells = stripped.split("│")
                # split 结果首尾有空串，去掉
                cells = [c for c in cells if c != "" or (cells.index(c) not in (0, len(cells) - 1))]
                # 更精确：去掉首尾空元素
                if cells and cells[0] == "":
                    cells = cells[1:]
                if cells and cells[-1] == "":
                    cells = cells[:-1]

                if cells:
                    data_rows.append((cells, is_header))

        # 合并跨行：
        # 情况1：首列空 → 追加到上一行各列
        # 情况2：首列非空但其余列全空 → 首列追加到上一行首列（标签换行）
        merged = []
        for cells, hdr in data_rows:
            if merged:
                prev_cells = merged[-1][0]
                # 首列空：追加到上一行各列
                if cells[0].strip() == "" and len(cells) == len(prev_cells):
                    for j in range(len(cells)):
                        prev_cells[j] += cells[j]
                    continue
                # 首列非空但其余列全空：标签换行，追加到上一行首列
                non_first_empty = all(c.strip() == "" for c in cells[1:])
                if non_first_empty and len(cells) == len(prev_cells):
                    prev_cells[0] += cells[0]
                    continue
            merged.append((cells[:], hdr))

        if not merged:
            return ""

        # 检测跨列行（├─┴─┤ 后面的行）：如果某行只有2列但表头有多列，标记 colspan
        num_cols = max(len(row[0]) for row in merged)

        # 渲染 HTML
        parts = ["<table>"]
        in_thead = True

        for cells, hdr in merged:
            # 补齐列数
            while len(cells) < num_cols:
                cells.append("")

            if in_thead and hdr:
                parts.append("<thead><tr>")
                for c in cells:
                    parts.append(_header_value(c))
                parts.append("</tr></thead><tbody>")
                in_thead = False
            else:
                if in_thead:
                    parts.append("<tbody>")
                    in_thead = False

                # 跨列检测：只有2列（label + value），且 num_cols > 2
                if num_cols > 2 and len([c for c in cells if c.strip()]) == 2:
                    # 检查是否大部分列为空
                    non_empty = sum(1 for c in cells if c.strip())
                    if non_empty <= 2:
                        label = cells[0]
                        value = "".join(cells[1:])
                        parts.append("<tr>")
                        parts.append(_header_value(label))
                        parts.append(f'<td colspan="{num_cols - 1}">{_esc(value.strip())}</td>')
                        parts.append("</tr>")
                        continue

                parts.append("<tr>")
                for c in cells:
                    parts.append(_cell_value(c))
                parts.append("</tr>")

        parts.append("</tbody></table>")
        return "\n".join(parts)

    @classmethod
    def _parse_announcement_block(cls, lines: list[str]) -> str:
        """将公告分隔块转为 HTML。

        格式：
        ─────────┬──────────────────
          2026-06-04 18:57│公告标题
        ─────────┴──────────────────
            公告正文...
        ─────────┬──────────────────
          2026-06-05 10:00│下一条标题
        ─────────┴──────────────────
            ...
        """
        parts = []
        current_date = ""
        current_title = ""
        body_lines = []
        in_body = False  # ┴ 之后收集正文

        for line in lines:
            stripped = line.strip()

            # ┬ 分隔线 = 新公告开始
            if stripped.startswith("─") and "┬" in stripped:
                # 输出上一条公告
                if current_title:
                    body_text = _linkify(_esc("\n".join(body_lines).strip()))
                    parts.append(
                        f'<div class="announcement">'
                        f'<span class="date">{_esc(current_date)}</span>'
                        f'<span class="title">{_esc(current_title)}</span>'
                        f'<div class="body">{body_text}</div>'
                        f'</div>'
                    )
                current_date = ""
                current_title = ""
                body_lines = []
                in_body = False
                continue

            # ┴ 分隔线 = 标题结束，接下来是正文
            if stripped.startswith("─") and "┴" in stripped:
                in_body = True
                continue

            # 标题行：日期│标题
            if not in_body and "│" in stripped and not stripped.startswith("│"):
                segments = stripped.split("│", 1)
                if len(segments) == 2:
                    current_date = segments[0].strip()
                    current_title = segments[1].strip()
                    continue

            # 正文行（┴ 之后）
            if in_body and stripped:
                body_lines.append(stripped)

        # 最后一条
        if current_title:
            body_text = _linkify(_esc("\n".join(body_lines).strip()))
            parts.append(
                f'<div class="announcement">'
                f'<span class="date">{_esc(current_date)}</span>'
                f'<span class="title">{_esc(current_title)}</span>'
                f'<div class="body">{body_text}</div>'
                f'</div>'
            )

        return "\n".join(parts)

    @classmethod
    def _is_table_start(cls, line: str) -> bool:
        """判断是否是标准表格开始（┌ 开头）。"""
        return line.strip().startswith("┌")

    @classmethod
    def _is_announcement_start(cls, line: str) -> bool:
        """判断是否是公告分隔块开始（───┬───，但不以 ┌ 开头）。"""
        s = line.strip()
        return s.startswith("─") and "┬" in s and not s.startswith("┌")

    @classmethod
    def _is_dash_separator(cls, line: str) -> bool:
        """判断是否是纯横线分隔（─────，不含 ┬┴┼┌└├┤）。"""
        s = line.strip()
        return (re.match(r'^─{5,}$', s) is not None
                and "┬" not in s and "┴" not in s and "┼" not in s)

    @classmethod
    def _parse_dash_table(cls, lines: list[str]) -> str:
        """将 ───── 分隔的空格对齐表格转为 HTML。

        这种表格用空格对齐、───── 做分隔线，列边界难以精确判断
        （股东名含空格、数值含空格），所以用 <pre> 保留原始对齐。
        """
        # 找所有 ───── 行的位置
        sep_positions = [i for i, l in enumerate(lines) if cls._is_dash_separator(l)]

        if not sep_positions:
            text = "\n".join(lines)
            return f'<p class="content">{_linkify(_esc(text))}</p>'

        parts = []

        # 第一个分隔线前的文本（可能是标题+列头）
        pre_text = "\n".join(l for l in lines[:sep_positions[0]] if l.strip())
        if pre_text:
            parts.append(f'<p class="content">{_linkify(_esc(pre_text))}</p>')

        # 每两个分隔线之间是一个数据块
        # 把所有分隔线之间的内容合并为 <pre>
        all_block_lines = []
        block_start = sep_positions[0] + 1
        for idx, sep_idx in enumerate(sep_positions):
            if idx == 0:
                continue
            # sep_positions[idx-1]+1 到 sep_idx 之间的行
            for k in range(sep_positions[idx - 1] + 1, sep_idx):
                if lines[k].strip():
                    all_block_lines.append(lines[k].rstrip())
            all_block_lines.append("")  # 空行分隔

        # 最后一个分隔线后的数据
        for k in range(sep_positions[-1] + 1, len(lines)):
            if lines[k].strip():
                all_block_lines.append(lines[k].rstrip())

        if all_block_lines:
            text = "\n".join(all_block_lines).rstrip()
            parts.append(f'<pre class="dash-table">{_linkify(_esc(text))}</pre>')

        return "\n".join(parts)

    @classmethod
    def _render_section(cls, section_name: str, section_text: str) -> str:
        """将一个板块的文本渲染为 HTML。"""
        html_parts = [f'<section id="{_esc(section_name)}">']
        html_parts.append(f"<h1>{_esc(section_name)}</h1>")

        # 去掉首行标题（如 '最新提示☆ ◇000001 平安银行 更新日期：...'）
        lines = section_text.split("\n")
        if lines and "◇" in lines[0]:
            lines = lines[1:]
        # 去掉 ★ 本栏包括 行
        if lines and lines[0].strip().startswith("★"):
            lines = lines[1:]
        # 第二行也可能是续行
        if lines and lines[0].strip().startswith("【") is False and lines[0].strip().startswith("─") is False and "【" not in lines[0]:
            # 可能是 ★ 行的续行，跳过
            if not lines[0].strip().startswith("【"):
                lines = lines[1:]

        # 逐块解析
        i = 0
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()

            # 空行
            if not stripped:
                i += 1
                continue

            # 子板块标题 【N.标题】
            if re.match(r'^【\d+', stripped):
                html_parts.append(f"<h2>{_esc(stripped)}</h2>")
                i += 1
                continue

            # 免责条款
            if "免责条款" in stripped:
                disclaimer_lines = [stripped]
                j = i + 1
                while j < len(lines) and lines[j].strip():
                    disclaimer_lines.append(lines[j].strip())
                    j += 1
                html_parts.append(
                    f'<div class="disclaimer">{_esc(" ".join(disclaimer_lines))}</div>'
                )
                i = j
                continue

            # 标准表格
            if cls._is_table_start(stripped):
                table_lines = [stripped]
                j = i + 1
                while j < len(lines):
                    nxt = lines[j].strip()
                    if nxt.startswith("└"):
                        table_lines.append(nxt)
                        j += 1
                        break
                    table_lines.append(nxt)
                    j += 1
                html_parts.append(cls._parse_table_block(table_lines))
                i = j
                continue

            # 公告分隔块
            if cls._is_announcement_start(stripped):
                ann_lines = [stripped]
                j = i + 1
                # 收集到下一个 ┴ 分隔线后的正文
                found_end = False
                while j < len(lines):
                    nxt = lines[j].strip()
                    ann_lines.append(nxt)
                    if "┴" in nxt:
                        found_end = True
                        # 继续收集正文直到下一个结构元素
                        j += 1
                        while j < len(lines):
                            nxt2 = lines[j].strip()
                            if (not nxt2 or
                                cls._is_table_start(nxt2) or
                                cls._is_announcement_start(nxt2) or
                                re.match(r'^【\d+', nxt2) or
                                "免责条款" in nxt2):
                                break
                            ann_lines.append(nxt2)
                            j += 1
                        break
                    j += 1
                html_parts.append(cls._parse_announcement_block(ann_lines))
                i = j
                continue

            # 普通文本段落（可能包含 ───── 分隔的表格）
            para_lines = [stripped]
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if (not nxt or
                    cls._is_table_start(nxt) or
                    cls._is_announcement_start(nxt) or
                    re.match(r'^【\d+', nxt) or
                    "免责条款" in nxt):
                    break
                para_lines.append(nxt)
                j += 1

            # 检查段落中是否有 ───── 分隔线，如果有则按 dash table 解析
            has_dash_sep = any(cls._is_dash_separator(l) for l in para_lines)
            if has_dash_sep:
                html_parts.append(cls._parse_dash_table(para_lines))
            else:
                text = "\n".join(para_lines)
                html_parts.append(f'<p class="content">{_linkify(_esc(text))}</p>')
            i = j

        html_parts.append("</section>")
        return "\n".join(html_parts)

    @classmethod
    def txt_to_html(cls, txt_content: str, code: str) -> str:
        """TXT → HTML 主函数。"""
        sections = cls._split_sections(txt_content)
        if not sections:
            return _HTML_TEMPLATE.format(code=code, name="", css=_CSS, nav="", body="")

        # 提取股票名
        name = cls._extract_stock_info(sections[0][1])

        # 生成导航
        nav_parts = ["<nav>"]
        for sec_name, _ in sections:
            nav_parts.append(
                f'<a href="#{_esc(sec_name)}">{_esc(sec_name)}</a>'
            )
        nav_parts.append("</nav>")

        # 渲染各板块
        body_parts = []
        for sec_name, sec_text in sections:
            body_parts.append(cls._render_section(sec_name, sec_text))

        return _HTML_TEMPLATE.format(
            code=code, name=name, css=_CSS,
            nav="\n".join(nav_parts),
            body="\n".join(body_parts),
        )

    # ---- Builder 协议 -------------------------------------------------------

    def build(self, order: BuildOrder) -> BuildResult:
        t0 = time.perf_counter()
        db = get_db()
        self._ensure_silver_schema()

        try:
            if order.mode == "full":
                rows_read, rows_written, issues = self._build_full(db)
            elif order.mode == "incremental":
                rows_read, rows_written, issues = self._build_incremental(db)
            elif order.mode == "repair":
                if not order.codes:
                    return BuildResult(
                        target=self.target, status="skipped",
                        errors=["repair mode requires codes"],
                        elapsed_ms=int((time.perf_counter() - t0) * 1000),
                    )
                rows_read, rows_written, issues = self._build_repair(db, order.codes)
            else:
                return BuildResult(
                    target=self.target, status="skipped",
                    errors=[f"unsupported mode: {order.mode}"],
                    elapsed_ms=int((time.perf_counter() - t0) * 1000),
                )
        except Exception as e:
            _log.error("f10_doc build failed: %s", e)
            return BuildResult(
                target=self.target, status="fail",
                errors=[str(e)],
                elapsed_ms=int((time.perf_counter() - t0) * 1000),
            )

        warnings = [i.message for i in issues if i.level == "warning"]
        errors = [i.message for i in issues if i.level == "error"]
        status = "ok" if not errors else ("partial" if rows_written > 0 else "fail")

        return BuildResult(
            target=self.target,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=len(errors),
            elapsed_ms=int((time.perf_counter() - t0) * 1000),
            errors=errors,
            warnings=warnings,
        )

    # ---- 数据读取 -----------------------------------------------------------

    def _read_bronze(self, db, codes: list[str] | None = None) -> pd.DataFrame:
        """读取 Bronze F10 元数据表。"""
        where = ""
        if codes:
            in_list = ",".join(f"'{c}'" for c in codes)
            where = f" WHERE code IN ({in_list})"

        df = db.execute(
            f"SELECT code, file_path, file_hash FROM bronze.raw_tdx_f10{where}",
            mode="read",
        )
        return df

    # ---- 文件转换与写入 -----------------------------------------------------

    def _convert_one(self, code: str, src_path: str, source_hash: str) -> dict | None:
        """转换单个文件：读 txt → 写 html → 返回元数据。"""
        if not os.path.exists(src_path):
            _log.warning("f10 source file not found: %s", src_path)
            return None

        with open(src_path, "r", encoding="utf-8") as f:
            txt_content = f.read()

        html = self.txt_to_html(txt_content, code)

        # 输出路径
        out_dir = "data/silver/f10"
        os.makedirs(out_dir, exist_ok=True)

        # 从源文件名提取日期，如 000001_20260622.txt → 2026-06-22
        fname = os.path.basename(src_path)
        m = re.search(r'(\d{4})(\d{2})(\d{2})', fname)
        if m:
            trade_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        else:
            trade_date = date.today().isoformat()

        out_path = f"{out_dir}/{code}_{trade_date.replace('-', '')}.html"

        html_bytes = html.encode("utf-8")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)

        file_hash = hashlib.sha256(html_bytes).hexdigest()[:16]

        return {
            "stock_code": code,
            "trade_date": trade_date,
            "file_path": out_path,
            "file_size": len(html_bytes),
            "file_hash": file_hash,
            "source_hash": source_hash,
            "source_system": "tdx",
        }

    def _write_metadata(self, db, records: list[dict]) -> int:
        """将元数据写入 silver.f10_doc，ON CONFLICT DO NOTHING。"""
        if not records:
            return 0

        df = pd.DataFrame(records)
        now = datetime.now()
        df["ingested_at"] = now

        # 确保 silver.f10_doc 表存在
        try:
            db.execute("SELECT 1 FROM silver.f10_doc LIMIT 1", mode="read")
        except Exception:
            db.create_table("silver.f10_doc", df)
            db.execute(
                "ALTER TABLE silver.f10_doc "
                "ADD PRIMARY KEY (stock_code, trade_date)",
                mode="write",
            )

        db.execute(
            "INSERT INTO silver.f10_doc SELECT * FROM _tmp "
            "ON CONFLICT (stock_code, trade_date) DO NOTHING",
            mode="write", df=df,
        )
        return len(records)

    # ---- full 模式 ----------------------------------------------------------

    def _build_full(self, db) -> tuple[int, int, list[QualityIssue]]:
        """全量转换。"""
        meta = self._read_bronze(db)
        rows_read = len(meta)
        if rows_read == 0:
            return 0, 0, []

        db.drop_table("silver.f10_doc")

        records = []
        for _, row in meta.iterrows():
            r = self._convert_one(row["code"], row["file_path"], row["file_hash"])
            if r:
                records.append(r)

        rows_written = self._write_metadata(db, records)
        issues = self._check_quality(db)
        _log.info("f10_doc full: read=%d written=%d", rows_read, rows_written)
        return rows_read, rows_written, issues

    # ---- incremental 模式 ---------------------------------------------------

    def _build_incremental(self, db) -> tuple[int, int, list[QualityIssue]]:
        """增量：只转 source_hash 变化的文件。"""
        meta = self._read_bronze(db)
        rows_read = len(meta)
        if rows_read == 0:
            return 0, 0, []

        # 已有的 source_hash
        try:
            existing = db.execute(
                "SELECT stock_code, source_hash FROM silver.f10_doc",
                mode="read",
            )
            existing_set = set(zip(existing["stock_code"], existing["source_hash"]))
        except Exception:
            # 表不存在，fallback 到 full
            return self._build_full(db)

        records = []
        for _, row in meta.iterrows():
            key = (row["code"], row["file_hash"])
            if key in existing_set:
                continue  # 源文件未变，跳过
            r = self._convert_one(row["code"], row["file_path"], row["file_hash"])
            if r:
                records.append(r)

        if not records:
            _log.info("f10_doc incremental: no new files to convert")
            return rows_read, 0, []

        rows_written = self._write_metadata(db, records)
        issues = self._check_quality(db)
        _log.info("f10_doc incremental: read=%d converted=%d", rows_read, rows_written)
        return rows_read, rows_written, issues

    # ---- repair 模式 --------------------------------------------------------

    def _build_repair(self, db, codes: list[str]) -> tuple[int, int, list[QualityIssue]]:
        """补数据：指定股票重新转换。"""
        meta = self._read_bronze(db, codes=codes)
        rows_read = len(meta)
        if rows_read == 0:
            return 0, 0, []

        try:
            db.execute("SELECT 1 FROM silver.f10_doc LIMIT 1", mode="read")
        except Exception:
            return self._build_full(db)

        # 删旧数据
        in_list = ",".join(f"'{c}'" for c in codes)
        db.execute(
            f"DELETE FROM silver.f10_doc WHERE stock_code IN ({in_list})",
            mode="write",
        )

        records = []
        for _, row in meta.iterrows():
            r = self._convert_one(row["code"], row["file_path"], row["file_hash"])
            if r:
                records.append(r)

        rows_written = self._write_metadata(db, records)
        issues = self._check_quality(db)
        _log.info("f10_doc repair: codes=%s converted=%d", codes, rows_written)
        return rows_read, rows_written, issues

    # ---- 质量检查 -----------------------------------------------------------

    def _check_quality(self, db) -> list[QualityIssue]:
        issues = []
        try:
            null_path = db.execute(
                "SELECT COUNT(*) AS c FROM silver.f10_doc WHERE file_path IS NULL",
                mode="read",
            ).iloc[0, 0]
            if null_path > 0:
                issues.append(QualityIssue(
                    level="warning", table="silver.f10_doc",
                    stock_code="", trade_date="", column="file_path",
                    message=f"file_path 为空 {null_path} 条",
                    raw_value=str(null_path),
                ))

            dup = db.execute(
                "SELECT stock_code, trade_date, COUNT(*) AS cnt "
                "FROM silver.f10_doc GROUP BY stock_code, trade_date "
                "HAVING cnt > 1 LIMIT 10",
                mode="read",
            )
            if not dup.empty:
                issues.append(QualityIssue(
                    level="error", table="silver.f10_doc",
                    stock_code="", trade_date="", column="",
                    message=f"存在 {len(dup)} 组重复 (stock_code, trade_date)",
                    raw_value=str(len(dup)),
                ))
        except Exception as e:
            _log.warning("quality check failed: %s", e)

        return issues
