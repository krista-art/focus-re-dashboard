#!/usr/bin/env python3
"""
Fetches action-item counts from Notion Transactions database
and writes them to data.json for the Focus RE dashboard.

Required environment variables:
  NOTION_TOKEN       — your Notion integration token (ntn_...)
  TRANSACTIONS_DB    — Notion database ID for transactions

Filters based on actual Notion schema:
  - Intercompany  : Intercompany checkbox = true  AND Reconciled = false
  - Credit Card   : Card relation is not empty    AND Reconciled = false
  - Banking       : Card relation is empty        AND Intercompany = false AND Reconciled = false
  - COUPA         : PLACE Reimbursable = true     AND PLACE Status ≠ "Reimbursed"
"""

import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone

NOTION_TOKEN    = os.environ["NOTION_TOKEN"]
TRANSACTIONS_DB = os.environ["TRANSACTIONS_DB"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


def query_database(db_id: str, filter_body: dict) -> int:
    """Query a Notion database with a filter and return the total result count."""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"

    count = 0
    has_more = True
    start_cursor = None

    while has_more:
        body = {"filter": filter_body, "page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor

        req = urllib.request.Request(
            url,
            data=json.dumps(body).encode(),
            headers=HEADERS,
            method="POST"
        )
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            print(f"Notion API error {e.code}: {e.read().decode()}")
            raise

        count += len(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return count


def main():
    # ------------------------------------------------------------------
    # 1. Pending Intercompany
    #    Intercompany checkbox = true AND Reconciled checkbox = false
    # ------------------------------------------------------------------
    intercompany_filter = {
        "and": [
            {"property": "Intercompany", "checkbox": {"equals": True}},
            {"property": "Reconciled",   "checkbox": {"equals": False}}
        ]
    }

    # ------------------------------------------------------------------
    # 2. Credit Card — linked to a Card record AND not yet reconciled
    #    Card relation is not empty AND Reconciled = false
    # ------------------------------------------------------------------
    cc_filter = {
        "and": [
            {"property": "Card",       "relation": {"is_not_empty": True}},
            {"property": "Reconciled", "checkbox": {"equals": False}}
        ]
    }

    # ------------------------------------------------------------------
    # 3. Banking — no card linked, not intercompany, not reconciled
    #    Card relation is empty AND Intercompany = false AND Reconciled = false
    # ------------------------------------------------------------------
    banking_filter = {
        "and": [
            {"property": "Card",         "relation": {"is_empty": True}},
            {"property": "Intercompany", "checkbox": {"equals": False}},
            {"property": "Reconciled",   "checkbox": {"equals": False}}
        ]
    }

    # ------------------------------------------------------------------
    # 4. COUPA Pending Reimbursements
    #    PLACE Reimbursable = true AND PLACE Status ≠ "Reimbursed"
    # ------------------------------------------------------------------
    coupa_filter = {
        "and": [
            {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
            {"property": "PLACE Status",       "select":   {"does_not_equal": "Reimbursed"}}
        ]
    }

    print("Fetching Intercompany count...")
    intercompany = query_database(TRANSACTIONS_DB, intercompany_filter)
    print(f"  → {intercompany}")

    print("Fetching Credit Card count...")
    cc = query_database(TRANSACTIONS_DB, cc_filter)
    print(f"  → {cc}")

    print("Fetching Banking count...")
    banking = query_database(TRANSACTIONS_DB, banking_filter)
    print(f"  → {banking}")

    # ------------------------------------------------------------------
    # 5. Uncategorized — Category = "Uncategorized" AND Reconciled = false
    # ------------------------------------------------------------------
    uncategorized_filter = {
        "and": [
            {"property": "Category",   "select":   {"equals": "Uncategorized"}},
            {"property": "Reconciled", "checkbox": {"equals": False}}
        ]
    }

    print("Fetching COUPA count...")
    coupa = query_database(TRANSACTIONS_DB, coupa_filter)
    print(f"  → {coupa}")

    print("Fetching Uncategorized count...")
    uncategorized = query_database(TRANSACTIONS_DB, uncategorized_filter)
    print(f"  → {uncategorized}")

    result = {
        "intercompany":  intercompany,
        "credit_card":   cc,
        "banking":       banking,
        "coupa":         coupa,
        "uncategorized": uncategorized,
        "updated_at":    datetime.now(timezone.utc).isoformat()
    }

    with open("data.json", "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n✅ data.json written: {result}")


if __name__ == "__main__":
    main()
