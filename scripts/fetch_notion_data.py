#!/usr/bin/env python3
"""
Fetches action-item counts from Notion Transactions database
and writes them to data.json for the Focus RE dashboard.

Required environment variables:
  NOTION_TOKEN       — your Notion integration token (ntn_...)
  TRANSACTIONS_DB    — Notion database ID for transactions
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

# PLACE Status options confirmed from Notion API:
# "N/A", "Draft", "Submitted", "Accepted", "Reimbursed"
# "Accepted" and "Reimbursed" = done. Everything else = needs action.
DONE_STATUSES = ["Accepted", "Reimbursed"]


def not_done_filter():
    """Returns a compound filter: PLACE Status is not Accepted AND not Reimbursed."""
    return {
        "and": [
            {"property": "PLACE Status", "select": {"does_not_equal": status}}
            for status in DONE_STATUSES
        ]
    }


def query_database(db_id: str, filter_body: dict) -> int:
    """Query a Notion database with a filter and return the total count."""
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
    # 1. Pending Intercompany — Category = "Intercompany" AND not done
    # ------------------------------------------------------------------
    intercompany_filter = {
        "and": [
            {"property": "Category", "select": {"equals": "Intercompany"}},
            *not_done_filter()["and"]
        ]
    }

    # ------------------------------------------------------------------
    # 2. Credit Card — Type = "Credit Card" AND not done
    # ------------------------------------------------------------------
    cc_filter = {
        "and": [
            {"property": "Type", "select": {"equals": "Credit Card"}},
            *not_done_filter()["and"]
        ]
    }

    # ------------------------------------------------------------------
    # 3. Banking — Type = "Banking" AND not done
    # ------------------------------------------------------------------
    banking_filter = {
        "and": [
            {"property": "Type", "select": {"equals": "Banking"}},
            *not_done_filter()["and"]
        ]
    }

    # ------------------------------------------------------------------
    # 4. COUPA — PLACE Status = "Submitted"
    # ------------------------------------------------------------------
    coupa_filter = {
        "property": "PLACE Status",
        "select": {"equals": "Submitted"}
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

    print("Fetching COUPA count...")
    coupa = query_database(TRANSACTIONS_DB, coupa_filter)
    print(f"  → {coupa}")

    result = {
        "intercompany": intercompany,
        "credit_card": cc,
        "banking": banking,
        "coupa": coupa,
        "updated_at": datetime.now(timezone.utc).isoformat()
    }

    with open("data.json", "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n✅ data.json written: {result}")


if __name__ == "__main__":
    main()
