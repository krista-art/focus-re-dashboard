#!/usr/bin/env python3
"""
Fetches action-item counts from Notion Transactions database and writes
them to data.json for the Focus RE dashboard.

Also auto-updates the Pending Count on each Credit Card page in Notion.

Required environment variables:
  NOTION_TOKEN    -- your Notion integration token (ntn_...)
  TRANSACTIONS_DB -- Notion database ID for transactions

Filters:
  - COUPA        : PLACE Reimbursable = true AND PLACE Status != Reimbursed AND PLACE Status != Accepted
  - Intercompany : Intercompany = true AND Reconciled = false
  - Credit Card  : (PLACE Reimbursable = true AND Reconciled = false) OR (Intercompany = true AND Reconciled = false)
  - Banking      : Card empty AND Intercompany = false AND Reconciled = false AND PLACE Reimbursable = false
  - Uncategorized: Category = Uncategorized AND Reconciled = false
"""

import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
TRANSACTIONS_DB = os.environ["TRANSACTIONS_DB"]

# last4 -> Notion page ID for each credit card
CARDS = {
    "9197": "318f7b005e9781e1bcd2dd299f5353f7",
    "1678": "318f7b005e978133ba7bd576da00462d",
    "1004": "318f7b005e97810c8594c57654408bed",
    "1006": "318f7b005e97816b9e4ac0a806ccedbd",
}

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


def query_database(db_id, filter_obj):
    """Return total number of pages matching filter_obj in db_id."""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    total = 0
    has_more = True
    start_cursor = None

    while has_more:
        payload = {"filter": filter_obj, "page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data, headers=HEADERS, method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                body = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            print(f"HTTP {e.code} querying {db_id}: {e.read().decode()}")
            raise

        total += len(body.get("results", []))
        has_more = body.get("has_more", False)
        start_cursor = body.get("next_cursor")

    return total


def update_page_property(page_id, prop_name, value):
    """Set a number property on a Notion page."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": {prop_name: {"number": value}}}
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers=HEADERS, method="PATCH")
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} updating page {page_id}: {e.read().decode()}")
        raise


def main():
    # --- Intercompany ---
    interco_filter = {
        "and": [
            {"property": "Intercompany", "checkbox": {"equals": True}},
            {"property": "Reconciled", "checkbox": {"equals": False}},
        ]
    }
    intercompany = query_database(TRANSACTIONS_DB, interco_filter)
    print(f"Intercompany: {intercompany}")

    # --- COUPA ---
    coupa_filter = {
        "and": [
            {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
            {"property": "PLACE Status", "select": {"does_not_equal": "Reimbursed"}},
            {"property": "PLACE Status", "select": {"does_not_equal": "Accepted"}},
        ]
    }
    coupa = query_database(TRANSACTIONS_DB, coupa_filter)
    print(f"COUPA: {coupa}")

    # --- Credit Card total ---
    cc_filter = {
        "or": [
            {
                "and": [
                    {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
                    {"property": "Reconciled", "checkbox": {"equals": False}},
                ]
            },
            {
                "and": [
                    {"property": "Intercompany", "checkbox": {"equals": True}},
                    {"property": "Reconciled", "checkbox": {"equals": False}},
                ]
            },
        ]
    }
    cc = query_database(TRANSACTIONS_DB, cc_filter)
    print(f"Credit Card total: {cc}")

    # --- Banking ---
    banking_filter = {
        "and": [
            {"property": "Card", "relation": {"is_empty": True}},
            {"property": "Intercompany", "checkbox": {"equals": False}},
            {"property": "Reconciled", "checkbox": {"equals": False}},
            {"property": "PLACE Reimbursable", "checkbox": {"equals": False}},
        ]
    }
    banking = query_database(TRANSACTIONS_DB, banking_filter)
    print(f"Banking: {banking}")

    # --- Uncategorized ---
    uncategorized_filter = {
        "and": [
            {"property": "Category", "select": {"equals": "Uncategorized"}},
            {"property": "Reconciled", "checkbox": {"equals": False}},
        ]
    }
    uncategorized = query_database(TRANSACTIONS_DB, uncategorized_filter)
    print(f"Uncategorized: {uncategorized}")

    # --- Update Pending Count on each Credit Card page ---
    for last4, page_id in CARDS.items():
        place_filter = {
            "and": [
                {"property": "Card", "relation": {"contains": page_id}},
                {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
                {"property": "Reconciled", "checkbox": {"equals": False}},
            ]
        }
        interco_card_filter = {
            "and": [
                {"property": "Card", "relation": {"contains": page_id}},
                {"property": "Intercompany", "checkbox": {"equals": True}},
                {"property": "Reconciled", "checkbox": {"equals": False}},
            ]
        }
        count = (
            query_database(TRANSACTIONS_DB, place_filter)
            + query_database(TRANSACTIONS_DB, interco_card_filter)
        )
        print(f"Card {last4}: {count} pending")
        update_page_property(page_id, "Pending Count", count)

    # --- Write data.json ---
    result = {
        "intercompany": intercompany,
        "credit_card": cc,
        "banking": banking,
        "coupa": coupa,
        "uncategorized": uncategorized,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    out_path = os.path.join(os.path.dirname(__file__), "..", "data.json")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    print("Wrote data.json:", result)


if __name__ == "__main__":
    main()
