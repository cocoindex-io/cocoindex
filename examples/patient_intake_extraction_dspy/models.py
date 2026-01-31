"""Pydantic models for patient intake form extraction."""

from __future__ import annotations

import datetime

from pydantic import BaseModel, Field


class Contact(BaseModel):
    """Emergency contact information."""

    name: str
    phone: str
    relationship: str


class Address(BaseModel):
    """Physical address."""

    street: str
    city: str
    state: str
    zip_code: str


class Pharmacy(BaseModel):
    """Pharmacy information."""

    name: str
    phone: str
    address: Address


class Insurance(BaseModel):
    """Insurance information."""

    provider: str
    policy_number: str
    group_number: str | None = None
    policyholder_name: str
    relationship_to_patient: str


class Condition(BaseModel):
    """Medical condition."""

    name: str
    diagnosed: bool


class Medication(BaseModel):
    """Current medication."""

    name: str
    dosage: str


class Allergy(BaseModel):
    """Known allergy."""

    name: str


class Surgery(BaseModel):
    """Past surgery."""

    name: str
    date: str


class Patient(BaseModel):
    """Complete patient information extracted from intake form."""

    name: str
    dob: datetime.date
    gender: str
    address: Address
    phone: str
    email: str
    preferred_contact_method: str
    emergency_contact: Contact
    insurance: Insurance | None = None
    reason_for_visit: str
    symptoms_duration: str
    past_conditions: list[Condition] = Field(default_factory=list)
    current_medications: list[Medication] = Field(default_factory=list)
    allergies: list[Allergy] = Field(default_factory=list)
    surgeries: list[Surgery] = Field(default_factory=list)
    occupation: str | None = None
    pharmacy: Pharmacy | None = None
    consent_given: bool
    consent_date: str | None = None
