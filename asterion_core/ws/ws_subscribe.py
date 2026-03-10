from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from asterion_core.clients.shared import as_str


TOKEN_ID_KEYS = {
    "asset_id",
    "assetId",
    "tokenId",
    "token_id",
    "clobTokenId",
    "clob_token_id",
}
TOKEN_ID_LIST_KEYS = {
    "clobTokenIds",
    "clob_token_ids",
    "tokenIds",
    "outcomeTokenIds",
}


def _decode_json_string(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    raw = value
    for _ in range(2):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            break
        if isinstance(decoded, str):
            raw = decoded
            continue
        return decoded
    return value


def _extract_token_ids_from_outcomes(outcomes: Any) -> set[str]:
    outcomes = _decode_json_string(outcomes)
    token_ids: set[str] = set()
    if outcomes is None:
        return token_ids

    if isinstance(outcomes, str):
        return {outcomes} if outcomes.isdigit() else token_ids
    if isinstance(outcomes, (int, float)):
        return {str(int(outcomes))}

    if isinstance(outcomes, Mapping):
        for key in TOKEN_ID_KEYS:
            value = as_str(outcomes.get(key))
            if value:
                token_ids.add(value)
        for key in TOKEN_ID_LIST_KEYS:
            values = outcomes.get(key)
            if isinstance(values, list):
                for item in values:
                    value = as_str(item)
                    if value:
                        token_ids.add(value)
        for value in outcomes.values():
            token_ids |= _extract_token_ids_from_outcomes(value)
        return token_ids

    if isinstance(outcomes, list):
        for item in outcomes:
            token_ids |= _extract_token_ids_from_outcomes(item)
        return token_ids

    return token_ids


def _extract_token_ids_from_market_raw(raw: Any) -> set[str]:
    raw = _decode_json_string(raw)
    token_ids: set[str] = set()
    if raw is None:
        return token_ids

    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if key in TOKEN_ID_KEYS:
                token_id = as_str(value)
                if token_id:
                    token_ids.add(token_id)
            elif key in TOKEN_ID_LIST_KEYS and isinstance(value, list):
                for item in value:
                    token_id = as_str(item)
                    if token_id:
                        token_ids.add(token_id)
            if isinstance(value, (dict, list, str)):
                token_ids |= _extract_token_ids_from_market_raw(value)
        return token_ids

    if isinstance(raw, list):
        for item in raw:
            token_ids |= _extract_token_ids_from_market_raw(item)
        return token_ids

    if isinstance(raw, (int, float)):
        return {str(int(raw))}

    return token_ids


def extract_token_ids_from_market_row(row: Mapping[str, Any]) -> set[str]:
    token_ids: set[str] = set()
    for key in TOKEN_ID_KEYS:
        value = as_str(row.get(key))
        if value:
            token_ids.add(value)

    for key in ("outcomes", "raw", "raw_json", "market_raw", "market_json"):
        token_ids |= _extract_token_ids_from_market_raw(row.get(key))

    return token_ids


def collect_token_ids(rows: Iterable[Mapping[str, Any]]) -> list[str]:
    token_ids: set[str] = set()
    for row in rows:
        token_ids |= extract_token_ids_from_market_row(row)
    return sorted(token_ids)


def load_token_ids_from_market_table(
    con: Any,
    *,
    table_name: str,
    token_id_column: str = "token_id",
    market_id_column: str = "market_id",
    condition_id_column: str = "condition_id",
    tradable_column: str | None = None,
    raw_json_column: str | None = None,
    outcomes_column: str | None = None,
    market_ids: list[str] | None = None,
    condition_ids: list[str] | None = None,
    tradable_only: bool = True,
) -> list[str]:
    selected_columns = [
        f"{token_id_column} AS token_id",
        f"{market_id_column} AS market_id",
        f"{condition_id_column} AS condition_id",
    ]
    if raw_json_column:
        selected_columns.append(f"{raw_json_column} AS raw")
    if outcomes_column:
        selected_columns.append(f"{outcomes_column} AS outcomes")

    where_clauses: list[str] = []
    params: list[Any] = []
    if market_ids:
        placeholders = ", ".join(["?"] * len(market_ids))
        where_clauses.append(f"{market_id_column} IN ({placeholders})")
        params.extend(market_ids)
    if condition_ids:
        placeholders = ", ".join(["?"] * len(condition_ids))
        where_clauses.append(f"{condition_id_column} IN ({placeholders})")
        params.extend(condition_ids)
    if tradable_only and tradable_column:
        where_clauses.append(f"{tradable_column} = ?")
        params.append(True)

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    rows = con.execute(
        f"SELECT {', '.join(selected_columns)} FROM {table_name}{where_sql}",
        params,
    ).fetchall()
    column_names = [column.split(" AS ")[-1] for column in selected_columns]
    records = [dict(zip(column_names, row, strict=False)) for row in rows]
    return collect_token_ids(records)


def load_token_ids_from_market_capabilities(
    con: Any,
    *,
    market_ids: list[str] | None = None,
    condition_ids: list[str] | None = None,
    tradable_only: bool = True,
) -> list[str]:
    return load_token_ids_from_market_table(
        con,
        table_name="capability.market_capabilities",
        token_id_column="token_id",
        market_id_column="market_id",
        condition_id_column="condition_id",
        tradable_column="tradable",
        market_ids=market_ids,
        condition_ids=condition_ids,
        tradable_only=tradable_only,
    )
