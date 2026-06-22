"""Generate synthetic SEC sample data: 10-K text filings + XBRL company facts."""

import json
from pathlib import Path

FILINGS = {
    "0000320193_2025-11-01_10-K.txt": """APPLE INC. FORM 10-K ANNUAL REPORT

Item 1A. Risk Factors

Cybersecurity threats are a material risk. A data breach or ransomware attack
on our systems could disrupt operations and harm our reputation. We continue to
invest in security to defend against cyber attacks.

Our supply chain is concentrated in a few regions; logistics disruptions or
component shortages could materially affect results.

Climate change and related environmental regulation may increase our costs and
affect demand. We have committed to reducing carbon emissions across operations.

We increasingly rely on artificial intelligence and machine learning in our
products; failure to keep pace could harm our competitive position.

Contact for investor relations: ir.contact@example.com, (555) 010-2003.
""",
    "0000789019_2025-10-15_10-K.txt": """MICROSOFT CORPORATION FORM 10-K ANNUAL REPORT

Item 1A. Risk Factors

Our cloud computing business (Azure) faces intense competition. Outages or a
data breach in our cloud services could result in regulatory enforcement and
loss of customer trust.

We are subject to extensive regulation and compliance obligations worldwide;
changes in regulation could increase costs.

Artificial intelligence is central to our strategy; ethical, legal, and
cybersecurity risks associated with AI could affect adoption.
""",
}

FACTS = {
    "0000320193.json": {
        "cik": 320193,
        "entityName": "Apple Inc.",
        "filingDate": "2025-11-01",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenues",
                    "units": {
                        "USD": [
                            {
                                "val": 391035000000,
                                "end": "2024-09-28",
                                "filed": "2024-11-01",
                            },
                            {
                                "val": 383285000000,
                                "end": "2023-09-30",
                                "filed": "2023-11-03",
                            },
                        ]
                    },
                },
                "NetIncomeLoss": {
                    "label": "Net Income",
                    "units": {
                        "USD": [
                            {
                                "val": 93736000000,
                                "end": "2024-09-28",
                                "filed": "2024-11-01",
                            }
                        ]
                    },
                },
                "ResearchAndDevelopmentExpense": {
                    "label": "R&D Expense",
                    "units": {
                        "USD": [
                            {
                                "val": 31370000000,
                                "end": "2024-09-28",
                                "filed": "2024-11-01",
                            }
                        ]
                    },
                },
            }
        },
    },
    "0000789019.json": {
        "cik": 789019,
        "entityName": "Microsoft Corporation",
        "filingDate": "2025-10-15",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenues",
                    "units": {
                        "USD": [
                            {
                                "val": 245122000000,
                                "end": "2024-06-30",
                                "filed": "2024-07-30",
                            }
                        ]
                    },
                },
                "NetIncomeLoss": {
                    "label": "Net Income",
                    "units": {
                        "USD": [
                            {
                                "val": 88136000000,
                                "end": "2024-06-30",
                                "filed": "2024-07-30",
                            }
                        ]
                    },
                },
            }
        },
    },
}


def create_sample_data() -> None:
    Path("data/filings").mkdir(parents=True, exist_ok=True)
    Path("data/company_facts").mkdir(parents=True, exist_ok=True)
    for name, text in FILINGS.items():
        Path("data/filings", name).write_text(text)
    for name, obj in FACTS.items():
        Path("data/company_facts", name).write_text(json.dumps(obj))
    print(f"Wrote {len(FILINGS)} filings + {len(FACTS)} company-facts JSON to data/")


if __name__ == "__main__":
    create_sample_data()
