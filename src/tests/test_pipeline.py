import xml.etree.ElementTree as ET
from parse_ccda import extract_patient_id
from merge_claims import merge_with_claims
from parse_ccda import extract_medications
import pandas as pd

#Verifies that the patient ID is correctly extracted from a C-CDA XML file.
def test_extract_patient_id():
    xml_str = """<?xml version="1.0"?>
    <ClinicalDocument xmlns="urn:hl7-org:v3">
      <recordTarget>
        <patientRole>
          <id extension="TESTPATIENT123" />
        </patientRole>
      </recordTarget>
    </ClinicalDocument>"""

    tree = ET.ElementTree(ET.fromstring(xml_str))
    patient_id = extract_patient_id(tree)
    assert patient_id == "TESTPATIENT123"

#Ensures medication details are correctly parsed from the Medications section.
def test_extract_medications():
    xml_str = """<?xml version="1.0"?>
    <ClinicalDocument xmlns="urn:hl7-org:v3">
      <component>
        <structuredBody>
          <component>
            <section>
              <code code="10160-0" />
              <entry>
                <substanceAdministration>
                  <consumable>
                    <manufacturedProduct>
                      <manufacturedMaterial>
                        <code code="12345" displayName="TestDrug" />
                      </manufacturedMaterial>
                    </manufacturedProduct>
                  </consumable>
                  <statusCode code="active" />
                  <effectiveTime value="20240101" />
                </substanceAdministration>
              </entry>
            </section>
          </component>
        </structuredBody>
      </component>
    </ClinicalDocument>"""

    tree = ET.ElementTree(ET.fromstring(xml_str))
    meds = extract_medications(tree)
    assert len(meds) == 1
    assert meds[0]["med_name"] == "TestDrug"
    assert meds[0]["rxnorm"] == "12345"
    assert meds[0]["status"] == "active"


#Confirms that medications and problems are properly joined with claims data on patient ID.
def test_merge_with_claims():
    meds_df = pd.DataFrame([{"patient_id": "123", "med_name": "DrugX"}])
    probs_df = pd.DataFrame([{"patient_id": "123", "problem_name": "Asthma"}])
    dx_claims = pd.DataFrame([{"MemberID": "123", "ClaimID": "C001"}])
    rx_claims = pd.DataFrame([{"MemberID": "123", "NDC": "00000"}])

    merged_meds, merged_probs = merge_with_claims(meds_df, probs_df, dx_claims, rx_claims)

    assert "NDC" in merged_meds.columns
    assert "ClaimID" in merged_probs.columns
    assert merged_meds.iloc[0]["patient_id"] == "123"