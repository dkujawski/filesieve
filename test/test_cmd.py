import sys

import pytest

from filesieve import cmd


def test_similarity_report_rejects_exact_mode(tmp_path, monkeypatch, capsys):
    base = tmp_path / "base"
    duplicate_dir = tmp_path / "duplicates"
    report = tmp_path / "similar.json"
    base.mkdir()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "filesieve",
            "--mode",
            "exact",
            "--report-similar",
            str(report),
            "--alternate",
            str(duplicate_dir),
            str(base),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cmd.main()

    assert exc_info.value.code == 2
    assert "--report-similar requires --mode media" in capsys.readouterr().err
    assert not report.exists()


def test_move_similar_rejects_exact_mode(tmp_path, monkeypatch, capsys):
    base = tmp_path / "base"
    duplicate_dir = tmp_path / "duplicates"
    base.mkdir()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "filesieve",
            "--mode",
            "exact",
            "--move-similar",
            "--alternate",
            str(duplicate_dir),
            str(base),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cmd.main()

    assert exc_info.value.code == 2
    assert "--move-similar requires --mode media" in capsys.readouterr().err
