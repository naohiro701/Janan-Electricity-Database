from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile
from io import BytesIO

from collectors.base import CollectedRaw
from config import ROOT_DIR, load_sources


FIXTURE_DIR = ROOT_DIR / "tests" / "fixtures"


def load_fixture_text(name: str) -> str:
    return (FIXTURE_DIR / "raw" / name).read_text(encoding="utf-8")


def build_reserve_zip_bytes() -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as archive:
        archive.writestr(
            "広域予備率ブロック情報_sample.csv",
            '\ufeff"2026/03/15 02:30 UPDATE"\n'
            '"対象年月日","時刻","ブロックNo","エリア名","広域ブロック需要(MW)","広域ブロック供給力(MW)","広域ブロック予備力(MW)","広域予備率(%)","広域使用率(%)","エリア需要(MW)","エリア供給力(MW)","エリア予備力(MW)"\n'
            '"2026/03/15","00:30","1","北海道","11079.008","13229.248","2150.24","19.41","83.75","3286","3628","342"\n'
            '"2026/03/15","01:00","1","北海道","11100.000","13300.000","2200.00","19.82","83.46","3300","3650","350"\n',
        )
        archive.writestr(
            "広域予備率連系線情報_sample.csv",
            '\ufeff"2026/03/15 02:30 UPDATE"\n'
            '"対象年月日","時刻","連系線名","順方向運用容量(MW)"\n'
            '"2026/03/15","00:30","北海道・本州間電力連系設備","900"\n',
        )
    return buffer.getvalue()


def build_intertie_bytes() -> bytes:
    return (
        '"連系線","対象日付","対象時刻","潮流実績"\n'
        '"北海道・本州間電力連系設備","2026/03/15","00:00","120.5"\n'
        '"北海道・本州間電力連系設備","2026/03/15","00:05","121.0"\n'
    ).encode("cp932")


def build_trunk_bytes() -> bytes:
    return (
        '"対象年月日","対象エリア","電圧","送電線名","潮流方向(正方向)","00:00","00:30","01:00"\n'
        '"2026/03/15","北海道","275kV","北本連系幹線","北→南","100","110","120"\n'
    ).encode("cp932")


def build_generation_bytes() -> bytes:
    return (
        '"発電所コード","エリア","発電所名","ユニット名","発電方式・燃種","対象日","00:30[kWh]","01:00[kWh]","01:30[kWh]","日量[kWh]","更新日時"\n'
        '"P001","北海道","サンプル発電所","1号機","水力","2026/03/15","1000","1100","1200","3300","2026/03/15 03:00"\n'
    ).encode("utf-8")


def sample_sources():
    return load_sources()


def make_collected_raw(source_name: str, filename: str, body: bytes) -> CollectedRaw:
    saved_path = FIXTURE_DIR / "raw" / filename
    return CollectedRaw(
        source_name=source_name,
        biz_date=date(2026, 3, 15),
        mode="now",
        saved_path=saved_path,
        raw_filename=filename,
        raw_sha256="deadbeef" * 8,
        raw_size_bytes=len(body),
        fetched_at=datetime(2026, 3, 15, 3, 0),
        encoding="utf-8",
        content_type="text/csv",
        http_status=200,
        body=body,
        metadata={},
    )
