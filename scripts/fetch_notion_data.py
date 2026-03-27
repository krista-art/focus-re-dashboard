#!/usr/bin/env python3
"""
Fetches action-item counts from Notion Transactions database and writes
them to data.json for the Focus RE dashboard.

Also auto-updates the Pending Count on each Credit Card page in Notion.

Required environment variables:
  NOTION_TOKEN    -- your Notion integration token (ntn_...)
  TRANSACTIONS_DB -- Notion database ID for transactions

Filters:
  - COUPA        : PLACE Reimbursable = true AND Reconciled = false AND Card not empty
  - Intercompany : Intercompany = true AND Reconciled = false AND Card not empty
  - Credit Card  : sum of per-card totals (ALL unreconciled per card, any type)
  - Banking      : hardcoded 0
  - Uncategorized: Category empty AND Reconciled = false AND Card not empty
                   AND PLACE Reimbursable = false AND Intercompany = false
  - Other Reimb. : hardcoded 0 (Russ & Matt section, manual for now)
"""

import os
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
TRANSACTIONS_DB = os.environ["TRANSACTIONS_DB"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# Credit card page IDs -- Pending Count on each card page is auto-updated each run
CARDS = {
    "9197": "318f7b005e9781e1bcd2dd299f5353f7",
    "1678": "318f7b005e978133ba7bd576da00462d",
    "1006": "318f7b005e97816b9e4ac0a806ccedbd",
}

CARD_LABELS = {
    "9197": "Chase Ink Unlimited (...9197)",
    "1678": "Chase Ink (...1678)",
    "1006": "AMEX (...1006)",
}


def query_database(db_id, filter_body):
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
            url, data=json.dumps(body).encode(), headers=HEADERS, method="POST"
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


def update_page_number(page_id, prop_name, value):
    """Update a number property on a Notion page."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": {prop_name: {"number": value}}}
    req = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers=HEADERS, method="PATCH"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        print(f"  Failed to update {page_id}: {e.code} {e.read().decode()}")


def main():
    # ------------------------------------------------------------------
    # 1. COUPA Reimbursements
    #    PLACE Reimbursable = true AND Reconciled = false AND Card not empty
    # ------------------------------------------------------------------
    coupa_filter = {
        "and": [
            {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
            {"property": "Reconciled",         "checkbox": {"equals": False}},
            {"property": "Card",               "relation": {"is_not_empty": True}},
        ]
    }

    # ------------------------------------------------------------------
    # 2. Intercompany
    #    Intercompany = true AND Reconciled = false AND Card not empty
    # ------------------------------------------------------------------
    intercompany_filter = {
        "and": [
            {"property": "Intercompany", "checkbox": {"equals": True}},
            {"property": "Reconciled",   "checkbox": {"equals": False}},
            {"property": "Card",         "relation": {"is_not_empty": True}},
        ]
    }

    # ------------------------------------------------------------------
    # 3. Uncategorized
    #    Card not empty AND Reconciled = false AND PLACE Reimbursable = false
    #    AND Intercompany = false AND Category is empty
    # ------------------------------------------------------------------
    uncategorized_filter = {
        "and": [
            {"property": "Card",               "relation": {"is_not_empty": True}},
            {"property": "Reconciled",         "checkbox": {"equals": False}},
            {"property": "PLACE Reimbursable", "checkbox": {"equals": False}},
            {"property": "Intercompany",       "checkbox": {"equals": False}},
            {"property": "Category",           "select":   {"is_empty": True}},
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
    # 4. Per-card counts -- ALL unreconciled transactions per card
    #    (includes COUPA, intercompany, and pure CC)
    # ------------------------------------------------------------------
    cards_data = {}
    cc_total = 0

    for last4, page_id in CARDS.items():
        print(f"Fetching totals for card ...{last4}...")

        # Total pending (all unreconciled) for this card
        card_all_filter = {
            "and": [
                {"property": "Card",       "relation": {"contains": page_id}},
                {"property": "Reconciled", "checkbox": {"equals": False}},
            ]
        }
        total = query_database(TRANSACTIONS_DB, card_all_filter)
        print(f"  total   -> {total}")

        # Intercompany sub-count for this card
        card_interco_filter = {
            "and": [
                {"property": "Card",         "relation": {"contains": page_id}},
                {"property": "Reconciled",   "checkbox": {"equals": False}},
                {"property": "Intercompany", "checkbox": {"equals": True}},
            ]
        }
        interco = query_database(TRANSACTIONS_DB, card_interco_filter)
        print(f"  interco -> {interco}")

        # COUPA sub-count for this card
        card_coupa_filter = {
            "and": [
                {"property": "Card",               "relation": {"contains": page_id}},
                {"property": "Reconciled",         "checkbox": {"equals": False}},
                {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
            ]
        }
        coupa_card = query_database(TRANSACTIONS_DB, card_coupa_filter)
        print(f"  coupa   -> {coupa_card}")

        # Uncategorized sub-count for this card
        card_uncat_filter = {
            "and": [
                {"property": "Card",               "relation": {"contains": page_id}},
                {"property": "Reconciled",         "checkbox": {"equals": False}},
                {"property": "PLACE Reimbursable", "checkbox": {"equals": False}},
                {"property": "Intercompany",       "checkbox": {"equals": False}},
                {"property": "Category",           "select":   {"is_empty": True}},
            ]
        }
        uncat = query_database(TRANSACTIONS_DB, card_uncat_filter)
        print(f"  uncat   -> {uncat}")

        cards_data[last4] = {
            "label":         CARD_LABELS.get(last4, f"Card ...{last4}"),
            "pending":       total,
            "interco":       interco,
            "coupa":         coupa_card,
            "uncategorized": uncat,
        }
        cc_total += total

    print(f"CC total: {cc_total}")

    # ------------------------------------------------------------------
    # 5. Write data.json
    # ------------------------------------------------------------------
    result = {
        "intercompany":         intercompany,
        "credit_card":          cc_total,
        "banking":              0,
        "coupa":                coupa,
        "other_reimbursements": 0,
        "uncategorized":        uncategorized,
        "cards":                cards_data,
        "updated_at":           datetime.now(timezone.utc).isoformat(),
    }

    with open("data.json", "w") as f:
        json.dump(result, f, indent=2)
    print(f"\n  data.json written: {result}")

    # ------------------------------------------------------------------
    # 6. Update Pending Count on each Credit Card page in Notion
    # ------------------------------------------------------------------
    print("\nUpdating Pending Count on Credit Card pages...")
    for last4, page_id in CARDS.items():
        count = cards_data[last4]["pending"]
        print(f"  Setting ...{last4} Pending Count = {count}")
        update_page_number(page_id, "Pending Count", count)

    print("\nDone.")


if __name__ == "__main__":
    main()
