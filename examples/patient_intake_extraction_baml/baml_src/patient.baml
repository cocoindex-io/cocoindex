// BAML Schema for Patient Intake Form Extraction

class Contact {
  name string
  phone string
  relationship string
}

class Address {
  street string
  city string
  state string
  zip_code string
}

class Pharmacy {
  name string
  phone string
  address Address
}

class Insurance {
  provider string
  policy_number string
  group_number string?
  policyholder_name string
  relationship_to_patient string
}

class Condition {
  name string
  diagnosed bool
}

class Medication {
  name string
  dosage string
}

class Allergy {
  name string
}

class Surgery {
  name string
  date string
}

class Patient {
  name string
  dob string
  gender string
  address Address
  phone string
  email string
  preferred_contact_method string
  emergency_contact Contact
  insurance Insurance?
  reason_for_visit string
  symptoms_duration string
  past_conditions Condition[]
  current_medications Medication[]
  allergies Allergy[]
  surgeries Surgery[]
  occupation string?
  pharmacy Pharmacy?
  consent_given bool
  consent_date string?
}

function ExtractPatientInfo(intake_form: pdf) -> Patient {
  client Gemini
  prompt #"
    Extract all patient information from the following intake form document.
    Please be thorough and extract all available information accurately.

    {{ intake_form }}

    Fill in with "N/A" for required fields if the information is not available.

    {{ ctx.output_format }}
  "#
}

client<llm> Gemini {
  provider google-ai
  options {
    model gemini-2.5-flash
    api_key env.GEMINI_API_KEY
  }
}
