#!/usr/bin/env python3
"""
Fetches action-item counts from Notion Transactions database
and writes them to data.json for the Focus RE dashboard.
Also auto-updates the Pending Count on each Credit Card page in Notion.

Required environment variables:
  NOTION_TOKEN      — your Notion integration token (ntn_...)
  TRANSACTIONS_DB   — Notion database ID for transactions

Filters (no overlap between cards):
  - COUPA          : PLACE Reimbursable = true AND Reconciled = false
  - Intercompany   : Intercompany = true AND Reconciled = false
  - Credit Card    : Card not empty AND Reconciled = false AND PLACE Reimbursable = false AND Intercompany = false
  - Banking        : hardcoded 0 (intercompany already covers these)
  - Uncategorized  : Category = "Uncategorized" AND Reconciled = false
  - Other Reimb.   : hardcoded 0 (Russ & Matt section, manual for now)
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

# Credit card page IDs — Pending Count on each card page is auto-updated each run
CARDS = {
    "9197": "318f7b005e9781e1bcd2dd299f5353f7",
    "1678": "318f7b005e978133ba7bd576da00462d",
    "1006": "318f7b005e97816b9e4ac0a806ccedbd",
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
                count += len(data.get("results", []))
                has_more = data.get("has_more", False)
                start_cursor = data.get("next_cursor")
        except urllib.error.HTTPError as e:
            print(f"  HTTP error {e.code}: {e.read().decode()}")
            return 0

    return count


def update_page_number(page_id: str, prop_name: str, value: int):
    """Update a number property on a Notion page."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": {prop_name: {"number": value}}}
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers=HEADERS,
        method="PATCH"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        print(f"  Failed to update {page_id}: {e.code} {e.read().decode()}")


def main():
    # ------------------------------------------------------------------
    # 1. COUPA Reimbursements
    #    PLACE Reimbursable = true AND Reconciled = false
    # ------------------------------------------------------------------
    coupa_filter = {
        "and": [
            {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
            {"property": "Reconciled",          "checkbox": {"equals": False}}
        ]
    }

    # ------------------------------------------------------------------
    # 2. Intercompany
    #    Intercompany = true AND Reconciled = false
    # ------------------------------------------------------------------
    intercompany_filter = {
        "and": [
            {"property": "Intercompany", "checkbox": {"equals": True}},
            {"property": "Reconciled",   "checkbox": {"equals": False}}
        ]
    }

    # ------------------------------------------------------------------
    # 3. Credit Card Review (global total)
    #    Card not empty AND Reconciled = false
    #    AND PLACE Reimbursable = false AND Intercompany = false
    # ------------------------------------------------------------------
    cc_filter = {
        "and": [
            {"property": "Card",               "relation":  {"is_not_empty": True}},
            {"property": "Reconciled",         "checkbox":  {"equals": False}},
            {"property": "PLACE Reimbursable", "checkbox":  {"equals": False}},
            {"property": "Intercompany",       "checkbox":  {"equals": False}}
        ]
    }

    # ------------------------------------------------------------------
    # 4. Uncategorized
    #    Card not empty AND Reconciled = false AND PLACE Reimbursable = false
    #    AND Intercompany = false AND Category is empty
    # ------------------------------------------------------------------
    uncategorized_filter = {
        "and": [
            {"property": "Card",               "relation":  {"is_not_empty": True}},
            {"property": "Reconciled",         "checkbox":  {"equals": False}},
            {"property": "PLACE Reimbursable", "checkbox":  {"equals": False}},
            {"property": "Intercompany",       "checkbox":  {"equals": False}},
            {"property": "Category",           "select":    {"is_empty": True}}
        ]
    }

    print("Fetching COUPA count...")
    coupa = query_database(TRANSACTIONS_DB, coupa_filter)
    print(f"  -> {coupa}")

    print("Fetching Intercompany count...")
    intercompany = query_database(TRANSACTIONS_DB, intercompany_filter)
    print(f"  -> {intercompany}")

    print("Fetching Uncategorized count...")
    uncategorized = query_database(TRANSACTIONS_DB, uncategorized_filter)
    print(f"  -> {uncategorized}")

    # ------------------------------------------------------------------
    # 5. Per-card counts (also used for global CC total)
    # ------------------------------------------------------------------
    cards_data = {}
    cc_total = 0

    CARD_LABELS = {
        "9197": "Chase Ink Unlimited (...9197)",
        "1678": "Chase Ink (...1678)",
        "1006": "AMEX (...1006)",
    }

    for last4, page_id in CARDS.items():
        print(f"Fetching CC pending for card ...{last4}...")
        card_filter = {
            "and": [
                {"property": "Card",               "relation":  {"contains": page_id}},
                {"property": "Reconciled",         "checkbox":  {"equals": False}},
                {"property": "PLACE Reimbursable", "checkbox":  {"equals": False}},
                {"property": "Intercompany",       "checkbox":  {"equals": False}}
            ]
        }
        count = query_database(TRANSACTIONS_DB, card_filter)
        print(f"  -> {count}")
        cards_data[last4] = {
            "label":   CARD_LABELS.get(last4, f"Card ...{last4}"),
            "pending": count
        }
        cc_total += count

    print(f"CC total: {cc_total}")

    # ------------------------------------------------------------------
    # 6. Write data.json
    # ------------------------------------------------------------------
    result = {
        "intercompany":        intercompany,
        "credit_card":         cc_total,
        "banking":             0,
        "coupa":               coupa,
        "other_reimbursements": 0,
        "uncategorized":       uncategorized,
        "cards":               cards_data,
        "updated_at":          datetime.now(timezone.utc).isoformat()
    }

    with open("data.json", "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n data.json written: {result}")

    # ------------------------------------------------------------------
    # 7. Update Pending Count on each Credit Card page in Notion
    # ------------------------------------------------------------------
    print("\nUpdating Pending Count on Credit Card pages...")
    for last4, page_id in CARDS.items():
        count = cards_data[last4]["pending"]
        print(f"  Setting ...{last4} Pending Count = {count}")
        update_page_number(page_id, "Pending Count", count)

    print("\nDone.")


if __name__ == "__main__":
    main()
