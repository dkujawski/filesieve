from __future__ import annotations

import os

from filesieve.organize import MediaOrganizer, OrganizerConfig, _parse_media_name, load_yaml_config


def test_yaml_config_loader(tmp_path):
    config = tmp_path / "organize.yaml"
    config.write_text(
        "\n".join(
            [
                "preset: plex",
                "duplicates_dir_name: DUPS",
                "unsorted_dir_name: UNSORTED",
                "dry_run: false",
            ]
        ),
        encoding="utf-8",
    )

    loaded = load_yaml_config(str(config))
    assert loaded.preset == "plex"
    assert loaded.duplicates_dir_name == "DUPS"
    assert loaded.unsorted_dir_name == "UNSORTED"
    assert loaded.dry_run is False


def test_filename_parser_handles_show_pattern(tmp_path):
    sample = tmp_path / "My.Show.S01E02.1080p.mkv"
    sample.write_bytes(b"payload")

    record = _parse_media_name(str(sample))
    assert record is not None
    assert record.title == "My Show"
    assert record.season == 1
    assert record.episode == 2
    assert record.resolution_score == 1080


def test_organizer_dry_run_groups_duplicates(tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()

    canonical = source / "Movie.Title.(2021).2160p.mkv"
    duplicate = source / "Movie.Title.(2021).720p.mkv"
    canonical.write_bytes(b"same")
    duplicate.write_bytes(b"same")

    runner = MediaOrganizer(
        sources=[str(source)],
        target_root=str(target),
        config=OrganizerConfig(dry_run=True),
        state_db=str(tmp_path / "state.sqlite"),
    )
    result = runner.run()
    runner.close()

    assert result["total"] == 2
    assert result["dry_run"] is True
    statuses = {item["status"] for item in result["operations"]}
    assert statuses == {"organized", "duplicate"}


def test_organizer_apply_is_idempotent(tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    source.mkdir()
    target.mkdir()

    movie = source / "Movie.Title.(2021).1080p.mkv"
    movie.write_bytes(b"movie")

    state_db = tmp_path / "state.sqlite"
    config = OrganizerConfig(dry_run=False)

    first = MediaOrganizer(
        sources=[str(source)],
        target_root=str(target),
        config=config,
        state_db=str(state_db),
        dry_run=False,
    )
    first_result = first.run()
    first.close()

    second = MediaOrganizer(
        sources=[str(source)],
        target_root=str(target),
        config=config,
        state_db=str(state_db),
        dry_run=False,
    )
    second_result = second.run()
    second.close()

    assert first_result["moved"] == 1
    assert second_result["moved"] == 0

    organized_file = target / "Movies" / "Movie Title (2021)" / "Movie Title (2021).mkv"
    assert organized_file.exists()
    assert not movie.exists()
    assert os.path.exists(state_db)


def test_organizer_ignores_nested_target_on_repeated_apply(tmp_path):
    source = tmp_path / "source"
    target = source / "organized"
    source.mkdir()

    movie = source / "Movie.Title.(2021).1080p.mkv"
    movie.write_bytes(b"movie")
    state_db = target / ".filesieve-organizer.sqlite"
    config = OrganizerConfig(dry_run=False)

    first = MediaOrganizer(
        sources=[str(source)],
        target_root=str(target),
        config=config,
        state_db=str(state_db),
        dry_run=False,
    )
    first_result = first.run()
    first.close()

    second = MediaOrganizer(
        sources=[str(source)],
        target_root=str(target),
        config=config,
        state_db=str(state_db),
        dry_run=False,
    )
    second_result = second.run()
    second.close()

    organized_file = target / "Movies" / "Movie Title (2021)" / "Movie Title (2021).mkv"
    assert first_result["moved"] == 1
    assert second_result["total"] == 0
    assert second_result["moved"] == 0
    assert organized_file.exists()
    assert not organized_file.with_name("Movie Title (2021) (2).mkv").exists()


def test_organizer_dry_run_does_not_create_target_or_state(tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    state_db = tmp_path / "state" / "organizer.sqlite"
    source.mkdir()
    (source / "Movie.Title.(2021).1080p.mkv").write_bytes(b"movie")

    runner = MediaOrganizer(
        sources=[str(source)],
        target_root=str(target),
        config=OrganizerConfig(dry_run=True),
        state_db=str(state_db),
        dry_run=True,
    )
    result = runner.run()
    runner.close()

    assert result["moved"] == 0
    assert result["operations"]
    assert not target.exists()
    assert not state_db.exists()
