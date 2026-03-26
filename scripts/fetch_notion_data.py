#!/usr/bin/env python3
"""
Fetches action-item counts from Notion Transactions database
and writes them to data.json for the Focus RE dashboard.

Required environment variables:
  NOTION_TOKEN       — your Notion integration token (ntn_...)
  TRANSACTIONS_DB    — Notion database ID for transactions

Filters (no overlap between cards):
  - COUPA         : PLACE Reimbursable = true AND PLACE Status not in (Reimbursed, Accepted)
  - Intercompany  : Intercompany = true AND Reconciled = false
#!/usr/bin/env python3
"""
Fetches action-item counts from Notion Transactions database
and writes them to data.json for the Focus RE dashboard.
  Also auto-updates the Pending Count on each Credit Card page in Notion.

  Required environment variables:
  NOTION_TOKEN       -- your Notion integration token (ntn_...)
  TRANSACTIONS_DB    -- Notion database ID for transactions
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

# Credit card page IDs -- Pending Count on each card page is auto-updated each run
CARDS = {
      "9197": "318f7b005e9781e1bcd2dd299f5353f7",
      "1678": "318f7b005e978133ba7bd576da00462d",
      "1004": "318f7b005e97810c8594c57654408bed",
      "1006": "318f7b005e97816b9e4ac0a806ccedbd",
}


def query_database(db_id, filter_body):
      url = f"https://api.notion.com/v1/databases/{db_id}/query"
      count = 0
      has_more = True
      start_cursor = None
      while has_more:
                body = {"filter": filter_body, "page_size": 100}
                if start_cursor:
                              body["start_cursor"] = start_cursor
                          req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=HEADERS, method="POST")
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


def update_page_property(page_id, prop_name, value):
      url = f"https://api.notion.com/v1/pages/{page_id}"
    body = {"properties": {prop_name: {"number": value}}}
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=HEADERS, method="PATCH")
    try:
              with urllib.request.urlopen(req) as resp:
                            resp.read()
    except urllib.error.HTTPError as e:
        print(f"  Warning: Could not update page {page_id}: {e.code} {e.read().decode()}")


def main():
      coupa_filter = {"and": [
          {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
          {"property": "PLACE Status", "select": {"does_not_equal": "Reimbursed"}},
          {"property": "PLACE Status", "select": {"does_not_equal": "Accepted"}}
]}
    intercompany_filter = {"and": [
              {"property": "Intercompany", "checkbox": {"equals": True}},
              {"property": "Reconciled",   "checkbox": {"equals": False}}
    ]}
    cc_filter = {"or": [
              {"and": [{"property": "PLACE Reimbursable", "checkbox": {"equals": True}}, {"property": "Reconciled", "checkbox": {"equals": False}}]},
              {"and": [{"property": "Intercompany", "checkbox": {"equals": True}}, {"property": "Reconciled", "checkbox": {"equals": False}}]}
    ]}
    banking_filter = {"and": [
              {"property": "Card",               "relation":  {"is_empty": True}},
              {"property": "Intercompany",       "checkbox":  {"equals": False}},
              {"property": "PLACE Reimbursable", "checkbox":  {"equals": False}},
              {"property": "Reconciled",         "checkbox":  {"equals": False}}
    ]}
    uncategorized_filter = {"and": [
              {"property": "Category",   "select":   {"equals": "Uncategorized"}},
              {"property": "Reconciled", "checkbox": {"equals": False}}
    ]}

    print("Fetching dashboard counts...")
    coupa         = query_database(TRANSACTIONS_DB, coupa_filter)
    intercompany  = query_database(TRANSACTIONS_DB, intercompany_filter)
    cc            = query_database(TRANSACTIONS_DB, cc_filter)
    banking       = query_database(TRANSACTIONS_DB, banking_filter)
    uncategorized = query_database(TRANSACTIONS_DB, uncategorized_filter)

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
          print(f"data.json written: {result}")

    # Update Pending Count on each Credit Card page in Notion
    print("Updating Pending Count on Credit Card pages...")
    for last4, page_id in CARDS.items():
              card_filter = {"and": [
                            {"property": "Card",       "relation":  {"contains": page_id}},
                            {"property": "Reconciled", "checkbox":  {"equals": False}}
              ]}
              count = query_database(TRANSACTIONS_DB, card_filter)
              print(f"  Card {last4}: {count} pending")
              update_page_property(page_id, "Pending Count", count)
          print("All done.")


if __name__ == "__main__":
      main()- Credit Card   : Card not empty AND Reconciled = false AND PLACE Reimbursable = false AND Intercompany = false
  - Banking       : Card empty AND Intercompany = false AND Reconciled = false AND PLACE Reimbursable = false
  - Uncategorized : Category = "Uncategorized" AND Reconciled = false
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
    # 1. COUPA Pending Reimbursements
    #    PLACE Reimbursable = true AND PLACE Status not in (Reimbursed, Accepted)
    #    These are submitted to PLACE but not yet paid back
    # ------------------------------------------------------------------
    coupa_filter = {
        "and": [
            {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
            {"property": "PLACE Status", "select": {"does_not_equal": "Reimbursed"}},
            {"property": "PLACE Status", "select": {"does_not_equal": "Accepted"}}
        ]
    }

    # ------------------------------------------------------------------
    # 2. Pending Intercompany
    #    Intercompany = true AND Reconciled = false
    # ------------------------------------------------------------------
    intercompany_filter = {
        "and": [
            {"property": "Intercompany", "checkbox": {"equals": True}},
            {"property": "Reconciled",   "checkbox": {"equals": False}}
        ]
    }

    # ------------------------------------------------------------------
    # 3. Credit Card Review
    #    Total card expenses needing reimbursement:
    #    (PLACE Reimbursable = true AND Reconciled = false)   ← COUPA items
    #    OR (Intercompany = true AND Reconciled = false)       ← Intercompany items
    #    = 35 COUPA + 8 intercompany = 43
    # ------------------------------------------------------------------
    cc_filter = {
        "or": [
            {
                "and": [
                    {"property": "PLACE Reimbursable", "checkbox": {"equals": True}},
                    {"property": "Reconciled",         "checkbox": {"equals": False}}
                ]
            },
            {
                "and": [
                    {"property": "Intercompany", "checkbox": {"equals": True}},
                    {"property": "Reconciled",   "checkbox": {"equals": False}}
                ]
            }
        ]
    }

    # ------------------------------------------------------------------
    # 4. Banking Review
    #    No card, not intercompany, not PLACE, not reconciled
    # ------------------------------------------------------------------
    banking_filter = {
        "and": [
            {"property": "Card",               "relation":  {"is_empty": True}},
            {"property": "Intercompany",       "checkbox":  {"equals": False}},
            {"property": "PLACE Reimbursable", "checkbox":  {"equals": False}},
            {"property": "Reconciled",         "checkbox":  {"equals": False}}
        ]
    }

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

    print("Fetching Intercompany count...")
    intercompany = query_database(TRANSACTIONS_DB, intercompany_filter)
    print(f"  → {intercompany}")

    print("Fetching Credit Card count...")
    cc = query_database(TRANSACTIONS_DB, cc_filter)
    print(f"  → {cc}")

    print("Fetching Banking count...")
    banking = query_database(TRANSACTIONS_DB, banking_filter)
    print(f"  → {banking}")

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
