"""Разбор push о входящем звонке."""

from types import SimpleNamespace

from custom_components.intersvyaz.push_parser import parse_push, resolve_door_uid


def _door(uid, *, main=False, shared=False, relay_id=None, mac="", porch=None):
    return SimpleNamespace(
        uid=uid,
        is_main=main,
        is_shared=shared,
        relay_id=relay_id,
        mac=mac,
        porch_num=porch,
        address="",
    )


def test_empty_envelope_push_is_call() -> None:
    payload = parse_push(
        {"fcmMessageId": "1", "from": "2", "priority": "high"},
        "persistent-id",
        None,
        {"app_data": {"crypto-key": "a", "google.source": "b", "subtype": "c"}},
    )
    assert payload["is_call"] is True


def test_push_with_unrelated_text_is_not_call() -> None:
    assert parse_push({"title": "Акция", "body": "Скидка"})["is_call"] is False


def test_push_with_call_text_is_call() -> None:
    assert parse_push({"title": "Входящий звонок"})["is_call"] is True


def test_resolve_by_relay_id() -> None:
    doors = [_door("a", relay_id=1), _door("b", relay_id=2)]
    assert resolve_door_uid(doors, {"relay_id": "2"}) == "b"


def test_resolve_falls_back_to_single_main_door() -> None:
    doors = [_door("a", main=True), _door("b", shared=True)]
    assert resolve_door_uid(doors, {}) == "a"


def test_resolve_ambiguous_returns_none() -> None:
    doors = [_door("a", main=True), _door("b", main=True)]
    assert resolve_door_uid(doors, {}) is None
