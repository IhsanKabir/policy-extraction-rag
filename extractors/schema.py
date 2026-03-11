from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    fare_rule = "fare_rule"
    airline_offer = "airline_offer"
    supplier_offer = "supplier_offer"
    circular = "circular"
    waiver_notice = "waiver_notice"
    policy_bulletin = "policy_bulletin"
    terms_page = "terms_page"
    other = "other"


class CabinType(str, Enum):
    economy = "economy"
    premium_economy = "premium_economy"
    business = "business"
    first = "first"
    mixed = "mixed"
    unknown = "unknown"


class PassengerType(str, Enum):
    adt = "adt"
    chd = "chd"
    inf = "inf"
    all = "all"
    unknown = "unknown"


class PolicyTopic(str, Enum):
    refund = "refund"
    reissue_change = "reissue_change"
    no_show = "no_show"
    go_show = "go_show"
    cancellation = "cancellation"
    waiver = "waiver"
    other = "other"


class PolicyAction(str, Enum):
    allowed = "allowed"
    not_allowed = "not_allowed"
    conditional = "conditional"
    unknown = "unknown"


class AppliesTo(str, Enum):
    ticket = "ticket"
    fare = "fare"
    tax = "tax"
    segment = "segment"
    coupon = "coupon"
    itinerary = "itinerary"
    passenger = "passenger"
    unknown = "unknown"


class PenaltyType(str, Enum):
    fixed_amount = "fixed_amount"
    percentage = "percentage"
    forfeiture = "forfeiture"
    free = "free"
    fare_difference_only = "fare_difference_only"
    tax_only = "tax_only"
    mixed = "mixed"
    unknown = "unknown"


class BeforeAfterDeparture(str, Enum):
    before_departure = "before_departure"
    after_departure = "after_departure"
    both = "both"
    not_applicable = "not_applicable"
    unknown = "unknown"


class Metadata(BaseModel):
    source_id: str = Field(..., description="Unique identifier for the source document")
    document_id: str = Field(..., description="Canonical id for the document, may match source_id")
    document_type: SourceType

    # Optional fields we will populate during ingestion
    carrier: Optional[str] = None
    supplier: Optional[str] = None
    offer_or_circular_id: Optional[str] = None
    market: Optional[str] = None
    route_scope: Optional[str] = None
    cabin: Optional[str] = None
    fare_family: Optional[str] = None
    pax_type: Optional[str] = None
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None
    travel_from: Optional[str] = None
    travel_to: Optional[str] = None
    ticketing_from: Optional[str] = None
    ticketing_to: Optional[str] = None
    section_title: Optional[str] = None
    source_url: Optional[str] = None
    source_filename: Optional[str] = None
    chunk_id: Optional[str] = None


class PolicyClauseRecord(BaseModel):
    # Identity and source
    record_id: str = Field(..., description="Unique identifier for the extracted clause record")
    source_id: str
    document_id: str
    document_type: SourceType
    offer_or_circular_id: Optional[str] = None
    source_url: Optional[str] = None
    source_filename: Optional[str] = None

    # Issuer / commercial context
    carrier: Optional[str] = None
    supplier: Optional[str] = None
    market: Optional[str] = None
    route_scope: Optional[str] = None
    cabin: Optional[CabinType] = None
    fare_family: Optional[str] = None
    pax_type: Optional[PassengerType] = None

    # Policy classification
    policy_topic: PolicyTopic
    action: PolicyAction
    applies_to: Optional[AppliesTo] = None

    # Penalty / financial terms
    penalty_type: Optional[PenaltyType] = None
    penalty_amount: Optional[float] = None
    penalty_currency: Optional[str] = None
    penalty_percent: Optional[float] = None
    fare_difference_required: Optional[bool] = None
    tax_refund_rule: Optional[str] = None

    # Timing / eligibility
    time_window: Optional[str] = None
    before_after_departure: Optional[BeforeAfterDeparture] = None
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None
    travel_from: Optional[str] = None
    travel_to: Optional[str] = None
    ticketing_from: Optional[str] = None
    ticketing_to: Optional[str] = None
    eligibility_conditions: List[str] = Field(default_factory=list)
    exceptions: List[str] = Field(default_factory=list)

    # Traceability / quality
    clause_text: str
    section_title: Optional[str] = None
    chunk_id: str
    citation_span: Optional[str] = None
    confidence: Optional[float] = None
    needs_review: bool = False
    notes: Optional[str] = None
