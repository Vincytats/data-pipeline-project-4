# scripts/pipeline_participants.py

import pandas as pd

# =========================
# CONFIG
# =========================

GOOGLE_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU/export?format=csv"

OUTPUT_FILE = "../processed_participant_data.csv"

# =========================
# LOAD DATA
# =========================

def load_data():
    print("Loading participant data from Google Sheets...")
    df = pd.read_csv(GOOGLE_SHEET_CSV)
    return df

# =========================
# CLEAN COLUMN NAMES
# =========================

def clean_columns(df):
    print("Cleaning column names...")

    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("[^A-Za-z0-9_]", "", regex=True)

    return df

# =========================
# CLEAN CATEGORICAL DATA
# =========================

def clean_categorical(df):
    print("Cleaning categorical fields...")

    categorical_columns = [
        "Gender",
        "Race",
        "Nationality",
        "Disability_YesNo",
        "Approved_for_Contracting",
        "Do_you_own_a_cellphone",
        "Government_Grant"
    ]

    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    return df

# =========================
# CREATE AGE GROUPS
# =========================

def create_age_groups(df):
    print("Creating age groups...")

    if "Date_of_Birth" in df.columns:
        df["Date_of_Birth"] = pd.to_datetime(df["Date_of_Birth"], errors='coerce')

        today = pd.Timestamp.today()

        df["Age"] = (today - df["Date_of_Birth"]).dt.days // 365

        def age_group(age):
            if pd.isna(age):
                return "Unknown"
            elif age < 18:
                return "Under 18"
            elif 18 <= age <= 24:
                return "Youth (18-24)"
            elif 25 <= age <= 34:
                return "Young Adult (25-34)"
            elif 35 <= age <= 44:
                return "Adult (35-44)"
            elif 45 <= age <= 54:
                return "Mid Age (45-54)"
            else:
                return "Senior (55+)"

        df["Participant_Age_Group"] = df["Age"].apply(age_group)

    return df

# =========================
# REMOVE DUPLICATES
# =========================

def remove_duplicates(df):
    print("Removing duplicates...")

    if "ID_number__Non_SA_Passport" in df.columns:
        df = df.drop_duplicates(subset=["ID_number__Non_SA_Passport"])
    else:
        df = df.drop_duplicates()

    return df

# =========================
# TRANSFORM DATA
# =========================

def transform_data(df):
    print("Transforming data...")

    # Drop empty rows
    df = df.dropna(how='all')

    # Convert date columns
    date_columns = [col for col in df.columns if "Date" in col]

    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

# =========================
# MAIN PIPELINE
# =========================

def run_pipeline():
    df = load_data()
    df = clean_columns(df)
    df = clean_categorical(df)
    df = create_age_groups(df)
    df = remove_duplicates(df)
    df = transform_data(df)

    # Save cleaned dataset
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Participant dataset ready: {OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
