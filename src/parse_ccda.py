import xml.etree.ElementTree as ET
import pandas as pd

NS = {'hl7': 'urn:hl7-org:v3'}

def extract_patient_id(tree):
    #Extract patientId from the XML file using the <patientRole><id> extension attribute
    root = tree.getroot()
    id_element = root.find(".//hl7:patientRole/hl7:id", NS)
    return id_element.attrib.get('extension') if id_element is not None else None

def extract_medications(tree):
    #Extract medications from the C-CDA XML tree
    medications = []
    for section in tree.findall(".//hl7:section", NS):
        code = section.find("hl7:code", NS)
        if code is not None and code.attrib.get("code") == "10160-0":
            entries = section.findall(".//hl7:substanceAdministration", NS)
            for entry in entries:
                med = {
                    "med_name": None,
                    "status": None,
                    "start_date": None,
                    "end_date": None,
                    "rxnorm": None
                }

                consumable = entry.find(".//hl7:manufacturedMaterial/hl7:code", NS)
                if consumable is not None:
                    med["med_name"] = consumable.attrib.get("displayName")
                    med["rxnorm"] = consumable.attrib.get("code")

                status = entry.find("hl7:statusCode", NS)
                if status is not None:
                    med["status"] = status.attrib.get("code")

                effective_times = entry.findall("hl7:effectiveTime", NS)
                if len(effective_times) >= 1:
                    med["start_date"] = effective_times[0].attrib.get("value")
                if len(effective_times) >= 2:
                    med["end_date"] = effective_times[1].attrib.get("value")

                medications.append(med)
    return medications

def extract_problems(tree):
    #Extract problems from the C-CDA XML tree
    problems = []
    for section in tree.findall(".//hl7:section", NS):
        code = section.find("hl7:code", NS)
        if code is not None and code.attrib.get("code") == "11450-4":
            entries = section.findall(".//hl7:entry", NS)
            for entry in entries:
                prob = {
                    "problem_name": None,
                    "status": None,
                    "start_date": None,
                    "code": None,
                    "code_system": None
                }

                obs = entry.find(".//hl7:observation", NS)
                if obs is not None:
                    problem_code = obs.find("hl7:code", NS)
                    if problem_code is not None:
                        prob["problem_name"] = problem_code.attrib.get("displayName")
                        prob["code"] = problem_code.attrib.get("code")
                        prob["code_system"] = problem_code.attrib.get("codeSystem")

                    status = obs.find("hl7:statusCode", NS)
                    if status is not None:
                        prob["status"] = status.attrib.get("code")

                    effective_time = obs.find("hl7:effectiveTime", NS)
                    if effective_time is not None:
                        prob["start_date"] = effective_time.attrib.get("value")

                    problems.append(prob)
    return problems

def parse_ccda_file(filepath):
    #Combines all the above into Dataframes
    tree = ET.parse(filepath)
    patient_id = extract_patient_id(tree)
    medications = extract_medications(tree)
    problems = extract_problems(tree)

    meds_df = pd.DataFrame(medications)
    probs_df = pd.DataFrame(problems)
    meds_df["patient_id"] = patient_id
    probs_df["patient_id"] = patient_id

    return meds_df, probs_df
