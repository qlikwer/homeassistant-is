"""Разбор FCM push Интерсвязи без зависимостей от Home Assistant.

Точный формат push провайдера заранее неизвестен, поэтому разбор терпимый:
берутся любые словари из аргументов callback'а, имена полей проверяются в
нескольких вариантах написания. Всё, что не распознано, доступно в событии
`intersvyaz_push_received` для настройки автоматизаций и доработки разбора.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

_DEVICE_KEYS = ("deviceId", "device_id", "device", "intercom_id", "intercomId", "mac")
_RELAY_KEYS = ("relayId", "relay_id", "relay")
_ADDRESS_KEYS = ("address",)
_ENTRANCE_KEYS = ("entrance", "porch", "porchNum", "porch_num")
_TITLE_KEYS = ("title", "header", "gcm.notification.title", "google.c.a.c_l")
_BODY_KEYS = ("body", "text", "message", "gcm.notification.body")
_TYPE_KEYS = ("type", "pushType", "push_type", "event", "action", "notificationType")

_CALL_TYPE_MARKERS = ("call", "intercom", "domofon", "звон", "вызов")
_CALL_TEXT_MARKERS = ("звон", "вызов", "домофон", "call")

# Служебные поля конверта FCM: сами по себе ничего не говорят о содержании.
_ENVELOPE_KEYS = frozenset(
    {"fcmMessageId", "from", "priority", "crypto-key", "encryption", "subtype",
     "collapse_key", "message_type", "app_data"}
)


def _first(mapping: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _flatten(args: Iterable[Any]) -> dict[str, Any]:
    """Слить словари из аргументов callback'а и их вложенные data/notification."""

    merged: dict[str, Any] = {}
    stack: list[Any] = list(args)
    while stack:
        item = stack.pop(0)
        if not isinstance(item, Mapping):
            continue
        for key, value in item.items():
            if key in ("data", "notification", "app_data") and isinstance(value, Mapping):
                stack.append(value)
            elif isinstance(key, str):
                merged.setdefault(key, value)
    return merged


def parse_push(*args: Any) -> dict[str, Any]:
    """Вернуть нормализованное описание push.

    Ключи результата: device_id, relay_id, address, entrance, title, body,
    push_type, is_call, fields (имена всех полей), data (плоский словарь).
    """

    data = _flatten(args)
    title = _first(data, _TITLE_KEYS)
    body = _first(data, _BODY_KEYS)
    push_type = _first(data, _TYPE_KEYS)
    device_id = _first(data, _DEVICE_KEYS)
    relay_id = _first(data, _RELAY_KEYS)
    entrance = _first(data, _ENTRANCE_KEYS)

    type_text = str(push_type or "").lower()
    text = f"{title or ''} {body or ''}".lower()
    is_call = (
        any(marker in type_text for marker in _CALL_TYPE_MARKERS)
        or any(marker in text for marker in _CALL_TEXT_MARKERS)
        or (relay_id is not None and not type_text and not text.strip())
        # Провайдер шлёт звонок push-ом без полезной нагрузки (проверено на
        # практике 30.09.2026): в теле только поля конверта FCM.
        or (
            not type_text
            and not text.strip()
            and not any(
                k not in _ENVELOPE_KEYS and not k.startswith("google.")
                for k in data
            )
        )
    )
    return {
        "device_id": str(device_id) if device_id is not None else None,
        "relay_id": str(relay_id) if relay_id is not None else None,
        "address": _first(data, _ADDRESS_KEYS),
        "entrance": str(entrance) if entrance is not None else None,
        "title": title,
        "body": body,
        "push_type": push_type,
        "is_call": is_call,
        "fields": sorted(data.keys()),
        "data": data,
    }


def resolve_door_uid(doors: Iterable[Any], payload: Mapping[str, Any]) -> str | None:
    """Подобрать домофон по данным push (relay_id, mac/device_id, подъезд, адрес)."""

    doors = list(doors)
    if not doors:
        return None

    relay_id = payload.get("relay_id")
    device_id = str(payload.get("device_id") or "").lower()
    entrance = payload.get("entrance")
    address = str(payload.get("address") or "").lower()

    if relay_id is not None:
        for door in doors:
            if str(getattr(door, "relay_id", None)) == relay_id:
                return door.uid
    if device_id:
        for door in doors:
            mac = str(getattr(door, "mac", "") or "").lower()
            if device_id in {mac, str(door.uid).lower()}:
                return door.uid
    if entrance is not None:
        matches = [d for d in doors if str(getattr(d, "porch_num", None)) == entrance]
        if len(matches) == 1:
            return matches[0].uid
    if address:
        matches = [
            d
            for d in doors
            if str(getattr(d, "address", "") or "").lower()
            and (
                address in str(d.address).lower() or str(d.address).lower() in address
            )
        ]
        if len(matches) == 1:
            return matches[0].uid
    if len(doors) == 1:
        return doors[0].uid
    # Push без адресных данных: звонят в основной домофон аккаунта.
    main = [d for d in doors if getattr(d, "is_main", False) and not getattr(d, "is_shared", False)]
    if len(main) == 1:
        return main[0].uid
    return None
