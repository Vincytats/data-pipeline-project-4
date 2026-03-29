# scripts/pipeline_participants.py

import pandas as pd
import os

# =========================
# CONFIG
# =========================

GOOGLE_SHEET_CSV = "https://docs.google.com/spreadsheets/d/1x2Uy8L1l0x10YBDLLjIk91shMlTXsMtEPapCssXN1iU/export?format=csv"

# Save directly in repo root
OUTPUT_FILE = "processed_participant_data.csv"

# =========================
# LOAD DATA
# =========================

def load_data():
    print("Loading participant data from Google Sheets...")
    try:
        df = pd.read_csv(GOOGLE_SHEET_CSV)
        print(f"Loaded {len(df)} rows")
        return df
    except Exception as e:
        print("⚠️ Failed to load data:", e)
        return pd.DataFrame()  # return empty dataframe

# =========================
# CLEAN COLUMN NAMES
# =========================

def clean_columns(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("[^A-Za-z0-9_]", "", regex=True)
    return df

# =========================
# CLEAN CATEGORICAL DATA
# =========================

def clean_categorical(df):
    categorical_columns = [
        "Gender",
        "Race",
        "Nationality"
    ]

    for col in categorical_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    return df

# =========================
# CREATE AGE GROUPS
# =========================

def create_age_groups(df):
    if "Date_of_Birth" in df.columns:
        df["Date_of_Birth"] = pd.to_datetime(df["Date_of_Birth"], errors='coerce')

        today = pd.Timestamp.today()
        df["Age"] = (today - df["Date_of_Birth"]).dt.days // 365

        def age_group(age):
            if pd.isna(age):
                return "Unknown"
            elif age < 18:
                return "Under 18"
            elif age <= 24:
                return "Youth (18-24)"
            elif age <= 34:
                return "Young Adult (25-34)"
            elif age <= 44:
                return "Adult (35-44)"
            elif age <= 54:
                return "Mid Age (45-54)"
            else:
                return "Senior (55+)"

        df["Participant_Age_Group"] = df["Age"].apply(age_group)

    return df

# =========================
# REMOVE DUPLICATES
# =========================

def remove_duplicates(df):
    if "ID_number__Non_SA_Passport" in df.columns:
        df = df.drop_duplicates(subset=["ID_number__Non_SA_Passport"])
    else:
        df = df.drop_duplicates()
    return df

# =========================
# MAIN PIPELINE
# =========================

def run_pipeline():
    df = load_data()

    if df.empty:
        print("⚠️ No data loaded — creating empty output file")
        pd.DataFrame().to_csv(OUTPUT_FILE, index=False)
        return

    df = clean_columns(df)
    df = clean_categorical(df)
    df = create_age_groups(df)
    df = remove_duplicates(df)

    # Drop empty rows
    df = df.dropna(how='all')

    # ✅ ALWAYS SAVE OUTPUT
    df.to_csv(OUTPUT_FILE, index=False)

    # Double check file exists
    if os.path.exists(OUTPUT_FILE):
        print(f"✅ File successfully created: {OUTPUT_FILE}")
    else:
        print("❌ File was NOT created!")

# =========================

if __name__ == "__main__":
    run_pipeline()
