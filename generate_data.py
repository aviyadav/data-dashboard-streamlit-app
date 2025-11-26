#!/usr/bin/env python3
"""
Script to generate synthetic data similar to the CSV files in the data folder.
"""

import polars as pl
import random
from datetime import datetime, timedelta
from pathlib import Path

# Set random seed for reproducibility
random.seed(42)

def generate_medical_records(num_rows=1000):
    """Generate synthetic medical records similar to medical_records.csv"""
    
    data = {
        "Patient_ID": list(range(1, num_rows + 1)),
        "Age": [random.randint(1, 120) for _ in range(num_rows)],
        "Gender": [random.choice(["Male", "Female"]) for _ in range(num_rows)],
        "Height_cm": [random.randint(140, 190) for _ in range(num_rows)],
        "Weight_kg": [round(random.uniform(20.0, 160.0), 1) for _ in range(num_rows)],
        "Blood_Pressure_Systolic": [random.randint(110, 160) for _ in range(num_rows)],
        "Blood_Pressure_Diastolic": [random.randint(70, 100) for _ in range(num_rows)],
        "Disease_Category": [random.choice([
            "Healthy", "Diabetes", "Hypertension", "Heart Disease", 
            "Cancer", "Stroke", "Nephrology"
        ]) for _ in range(num_rows)]
    }
    
    df = pl.DataFrame(data)
    return df


def generate_health_insurance_records(num_rows=10000):
    """Generate synthetic health insurance records similar to ntrarogyaseva.csv"""
    
    # Reference data
    castes = ["BC", "OC", "SC", "ST", "Minorities"]
    categories = {
        "M5": "CARDIOLOGY",
        "M6": "NEPHROLOGY",
        "S7": "CARDIAC AND CARDIOTHORACIC SURGERY",
        "S16": "COCHLEAR IMPLANT SURGERY",
        "M8": "ONCOLOGY",
        "S3": "GENERAL SURGERY"
    }
    
    surgeries = {
        "M5.1.2": "Management Of Acute MI With Angiogram",
        "M5.1.5": "Medical Management of Refractory Cardiac Failure",
        "M6.5": "Maintenance Hemodialysis For Crf",
        "S7.1.1.1": "Coronary Balloon Angioplasty with stent(00.45)",
        "S7.2.1.1": "Coronary Bypass Surgery",
        "S16.1.1": "Cochlear Implant Surgery",
        "M8.3.2": "Chemotherapy for Cancer",
        "S3.1.1": "Laparoscopic Cholecystectomy"
    }
    
    villages = [
        "Lolugu", "Borivanka", "Kapasakuddi", "Telikipenta", "Thandemvalasa",
        "Phasigangupeta", "Vallur", "Rajam", "Thurlapadu", "Pulipadu"
    ]
    
    mandals = [
        "Ponduru", "Kaviti", "Sarubujjili", "Srikakulam", "Pathapatnam",
        "Nandyal", "Bhoghapuram", "Kakumanu", "Guntur(C)", "Butchayyapeta"
    ]
    
    districts = [
        "Srikakulam", "Vizianagaram", "Vishakhapatnam", "Guntur", "Kurnool"
    ]
    
    hospitals = [
        ("Rims Govt. General Hospital, Srikakulam", "G", "SRIKAKULAM", "Srikakulam"),
        ("Govt General Hospital Kurnool", "G", "KURNOOL", "Kurnool"),
        ("Queens Nri Hospitals", "C", "Visakhapatnam", "Vishakhapatnam"),
        ("Karumuri Hospital", "C", "GUNTUR", "Guntur"),
        ("Ent Nursing Home", "C", "GUNTUR", "Guntur"),
        ("Apollo Hospital", "C", "VISAKHAPATNAM", "Vishakhapatnam")
    ]
    
    src_registration = ["D", "P"]
    
    # Generate data
    start_date = datetime(2013, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    data = []
    for i in range(1, num_rows + 1):
        category_code = random.choice(list(categories.keys()))
        category_name = categories[category_code]
        
        # Filter surgeries by category
        matching_surgeries = [(code, name) for code, name in surgeries.items() 
                             if code.startswith(category_code)]
        if matching_surgeries:
            surgery_code, surgery_name = random.choice(matching_surgeries)
        else:
            surgery_code = f"{category_code}.1"
            surgery_name = "General Procedure"
        
        hospital_info = random.choice(hospitals)
        
        # Generate dates
        preauth_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        surgery_date = preauth_date + timedelta(days=random.randint(1, 30))
        discharge_date = surgery_date + timedelta(days=random.randint(1, 30))
        claim_date = discharge_date + timedelta(days=random.randint(1, 1000))
        
        # Mortality
        mortality = random.choice(["YES", "NO"])
        mortality_date = discharge_date + timedelta(days=random.randint(1, 60)) if mortality == "YES" else None
        
        # Age considerations
        age_group = random.choices(
            [range(1, 18), range(18, 60), range(60, 100)],
            weights=[0.1, 0.6, 0.3]
        )[0]
        age = random.choice(age_group)
        
        if age < 18:
            sex = random.choice(["Male(Child)", "Female(Child)"])
        else:
            sex = random.choice(["Male", "Female"])
        
        # Amount
        preauth_amt = random.choice([12500, 30000, 40000, 115846, 520000])
        claim_amount = preauth_amt if random.random() > 0.2 else preauth_amt - random.randint(0, 10000)
        
        row = {
            "": i,
            "AGE": age,
            "SEX": sex,
            "CASTE_NAME": random.choice(castes),
            "CATEGORY_CODE": category_code,
            "CATEGORY_NAME": category_name,
            "SURGERY_CODE": surgery_code,
            "SURGERY": surgery_name,
            "VILLAGE": random.choice(villages),
            "MANDAL_NAME": random.choice(mandals),
            "DISTRICT_NAME": random.choice(districts),
            "PREAUTH_DATE": preauth_date.strftime("%d/%m/%Y %H:%M:%S"),
            "PREAUTH_AMT": preauth_amt,
            "CLAIM_DATE": claim_date.strftime("%d/%m/%Y %H:%M:%S"),
            "CLAIM_AMOUNT": claim_amount,
            "HOSP_NAME": hospital_info[0],
            "HOSP_TYPE": hospital_info[1],
            "HOSP_LOCATION": hospital_info[2],
            "HOSP_DISTRICT": hospital_info[3],
            "SURGERY_DATE": surgery_date.strftime("%d/%m/%Y %H:%M:%S"),
            "DISCHARGE_DATE": discharge_date.strftime("%d/%m/%Y %H:%M:%S") if discharge_date else "",
            "Mortality Y / N": mortality,
            "MORTALITY_DATE": mortality_date.strftime("%d/%m/%Y %H:%M:%S") if mortality_date else "",
            "SRC_REGISTRATION": random.choice(src_registration)
        }
        
        data.append(row)
    
    df = pl.DataFrame(data)
    return df


def main():
    """Main function to generate all data files"""
    
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    print("Generating synthetic medical records...")
    medical_df = generate_medical_records(num_rows=1000)
    output_path = data_dir / "medical_records_synthetic.csv"
    medical_df.write_csv(output_path)
    print(f"✓ Generated {len(medical_df)} medical records → {output_path}")
    
    print("\nGenerating synthetic health insurance records...")
    insurance_df = generate_health_insurance_records(num_rows=10000)
    output_path = data_dir / "health_insurance_synthetic.csv"
    insurance_df.write_csv(output_path)
    print(f"✓ Generated {len(insurance_df)} health insurance records → {output_path}")
    
    # Also generate parquet versions
    # print("\nGenerating Parquet versions...")
    # medical_df.write_parquet(data_dir / "medical_records_synthetic.parquet")
    # print(f"✓ Generated medical_records_synthetic.parquet")
    
    # insurance_df.write_parquet(data_dir / "health_insurance_synthetic.parquet")
    # print(f"✓ Generated health_insurance_synthetic.parquet")
    
    print("\n" + "="*60)
    print("Data generation complete!")
    print("="*60)
    print(f"\nGenerated files in {data_dir}:")
    for file in sorted(data_dir.glob("*_synthetic.*")):
        size = file.stat().st_size / (1024 * 1024)  # MB
        print(f"  - {file.name} ({size:.2f} MB)")


if __name__ == "__main__":
    main()
