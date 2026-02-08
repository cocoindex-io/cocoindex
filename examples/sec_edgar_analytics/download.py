"""
SEC EDGAR Sample Data Module

Creates sample SEC data in multiple formats for the tutorial:
- TXT: 10-K filing text (risk factors)
- JSON: Company facts (SEC XBRL format)
- PDF: Exhibit documents

Usage:
    from download import create_sample_data
    create_sample_data()
"""

import json
import shutil
from pathlib import Path


def create_sample_data() -> None:
    """
    Create sample SEC data for the tutorial.

    Creates synthetic sample data in data/ directory:
    - data/filings/*.txt - 10-K risk factor excerpts
    - data/company_facts/*.json - Financial metrics (XBRL format)
    - data/exhibits_pdf/*.pdf - Exhibit documents
    """
    # Check if sample directory exists with real data
    sample_dir = Path(__file__).parent / "sample"
    if sample_dir.exists() and list(sample_dir.glob("*/*.txt")):
        _copy_sample_data(sample_dir)
        return

    # Create synthetic sample data
    _create_synthetic_sample_data()


def _copy_sample_data(sample_dir: Path) -> None:
    """Copy pre-packaged sample data from sample/ to data/ directory."""
    data_dir = Path("data")

    # Create data directories
    for subdir in ["filings", "company_facts", "exhibits_pdf"]:
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)

    # Copy files
    copied = {"filings": 0, "company_facts": 0, "exhibits_pdf": 0}

    for subdir in copied.keys():
        src_dir = sample_dir / subdir
        dst_dir = data_dir / subdir
        if src_dir.exists():
            for f in src_dir.iterdir():
                if f.is_file():
                    shutil.copy2(f, dst_dir / f.name)
                    copied[subdir] += 1

    print("Created sample SEC data\n")
    _print_data_summary()


def _create_synthetic_sample_data() -> None:
    """Create synthetic sample data for offline demo."""
    # Create directories
    Path("data/filings").mkdir(parents=True, exist_ok=True)
    Path("data/company_facts").mkdir(parents=True, exist_ok=True)
    Path("data/exhibits_pdf").mkdir(parents=True, exist_ok=True)

    # TXT FILINGS (10-K Risk Factors)
    sample_filings = {
        "0000320193_2024-11-01_10-K.txt": """APPLE INC. FORM 10-K ANNUAL REPORT
ITEM 1A. RISK FACTORS

CYBERSECURITY RISKS
The Company faces significant cybersecurity risks, including potential data breaches,
ransomware attacks, and unauthorized access to our systems. We have invested heavily
in security infrastructure to mitigate these threats.

CLIMATE CHANGE
Climate change poses risks to our global supply chain and manufacturing operations.
We are committed to achieving carbon neutrality by 2030.

ARTIFICIAL INTELLIGENCE
We continue to invest in artificial intelligence and machine learning capabilities
to enhance our products and services including Siri and computational photography.

SUPPLY CHAIN
Our products are manufactured by outsourcing partners primarily in Asia. Supply chain
disruptions from logistics issues or geopolitical tensions could impact production.""",
        "0000789019_2024-10-15_10-K.txt": """MICROSOFT CORPORATION FORM 10-K ANNUAL REPORT
ITEM 1A. RISK FACTORS

CLOUD INFRASTRUCTURE
Our Azure cloud platform faces intense competition from AWS and Google Cloud.
Cybersecurity threats continue to evolve and require constant vigilance.

AI INTEGRATION
We are integrating artificial intelligence across our product portfolio.
Our Copilot AI assistant represents significant investment in machine learning.

REGULATORY COMPLIANCE
We operate in a complex regulatory environment. Privacy regulations like GDPR
require continuous compliance investment across all markets.""",
        "0000019617_2024-03-01_10-K.txt": """JPMORGAN CHASE & CO. FORM 10-K ANNUAL REPORT
ITEM 1A. RISK FACTORS

CREDIT RISK
The Company faces significant credit risk across its lending portfolio.
Economic downturns may result in increased loan defaults.

CYBERSECURITY
We face constant cybersecurity threats from criminal organizations.
We invest over $700 million annually in cybersecurity.

CLIMATE RISK
Climate change poses transition and physical risks to our business.
We have committed to align financing with Paris Agreement goals.""",
    }

    for filename, content in sample_filings.items():
        Path(f"data/filings/{filename}").write_text(content)

    # JSON COMPANY FACTS (SEC XBRL API format)
    sample_facts = {
        "0000320193.json": {
            "cik": "320193",
            "entityName": "APPLE INC",
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "label": "Revenues",
                        "description": "Amount of revenue recognized from goods sold, services rendered.",
                        "units": {
                            "USD": [
                                {
                                    "end": "2024-09-28",
                                    "val": 394328000000,
                                    "filed": "2024-11-01",
                                },
                                {
                                    "end": "2023-09-30",
                                    "val": 383285000000,
                                    "filed": "2023-11-03",
                                },
                            ]
                        },
                    },
                    "NetIncomeLoss": {
                        "label": "Net Income (Loss)",
                        "description": "The portion of profit or loss for the period.",
                        "units": {
                            "USD": [
                                {
                                    "end": "2024-09-28",
                                    "val": 93736000000,
                                    "filed": "2024-11-01",
                                },
                            ]
                        },
                    },
                }
            },
        },
        "0000789019.json": {
            "cik": "789019",
            "entityName": "MICROSOFT CORPORATION",
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "label": "Revenues",
                        "description": "Amount of revenue recognized from goods sold, services rendered.",
                        "units": {
                            "USD": [
                                {
                                    "end": "2024-06-30",
                                    "val": 245122000000,
                                    "filed": "2024-10-15",
                                },
                            ]
                        },
                    },
                }
            },
        },
        "0000019617.json": {
            "cik": "19617",
            "entityName": "JPMORGAN CHASE & CO",
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "label": "Revenues",
                        "description": "Amount of revenue recognized from goods sold, services rendered.",
                        "units": {
                            "USD": [
                                {
                                    "end": "2023-12-31",
                                    "val": 158104000000,
                                    "filed": "2024-03-01",
                                },
                            ]
                        },
                    },
                }
            },
        },
    }

    for facts_filename, facts_data in sample_facts.items():
        Path(f"data/company_facts/{facts_filename}").write_text(
            json.dumps(facts_data, indent=2)
        )

    # PDF EXHIBITS (minimal valid PDFs)
    sample_pdfs = {
        "0000320193_2024-ex21.pdf": "APPLE INC - SUBSIDIARIES\n\n1. Apple Sales International\n2. Apple Operations International\n3. Braeburn Capital, Inc.",
        "0000789019_2024-ex21.pdf": "MICROSOFT CORPORATION - SUBSIDIARIES\n\n1. LinkedIn Corporation\n2. GitHub, Inc.\n3. Nuance Communications",
        "0000019617_2024-ex21.pdf": "JPMORGAN CHASE & CO - SUBSIDIARIES\n\n1. JPMorgan Chase Bank, N.A.\n2. J.P. Morgan Securities LLC",
    }

    for filename, text_content in sample_pdfs.items():
        pdf_content = _create_minimal_pdf(text_content)
        Path(f"data/exhibits_pdf/{filename}").write_bytes(pdf_content)

    print("Created sample SEC data\n")
    _print_data_summary()


def _create_minimal_pdf(text: str) -> bytes:
    """Create a minimal valid PDF file containing the given text."""
    text_escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    content = f"""1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj

2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj

3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792]
   /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj

4 0 obj
<< /Length {len(text_escaped) + 50} >>
stream
BT
/F1 12 Tf
50 700 Td
({text_escaped}) Tj
ET
endstream
endobj

5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj

xref
0 6
0000000000 65535 f
0000000009 00000 n
0000000058 00000 n
0000000115 00000 n
0000000266 00000 n
0000000{350 + len(text_escaped):03d} 00000 n

trailer
<< /Size 6 /Root 1 0 R >>
startxref
{420 + len(text_escaped)}
%%EOF"""

    return b"%PDF-1.4\n" + content.encode("latin-1")


def _print_data_summary() -> None:
    """Print summary of created data files."""
    print("data/filings/ (TXT):")
    for f in sorted(Path("data/filings").glob("*.txt")):
        print(f"  {f.name}")

    print("\ndata/company_facts/ (JSON):")
    for f in sorted(Path("data/company_facts").glob("*.json")):
        print(f"  {f.name}")

    print("\ndata/exhibits_pdf/ (PDF):")
    for f in sorted(Path("data/exhibits_pdf").glob("*.pdf")):
        print(f"  {f.name}")
