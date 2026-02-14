from dataclasses import dataclass, asdict
from typing import List

@dataclass
class KPICard:
    title: str
    value: str | int | float

@dataclass
class KPIChart:
    id: str
    title: str
    fig_json: str

@dataclass
class KPIBundle:
    engine: str
    cards: List[KPICard]
    charts: List[KPIChart]

def bundle_to_dict(bundle: KPIBundle) -> dict:
    """Convert KPIBundle to a JSON-serializable dict for API responses."""
    return asdict(bundle)
