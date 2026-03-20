import pandas as pd
import os
from openpyxl import load_workbook
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ─────────────────────────────────────────────
# Put your Molina CSV files in data/raw/
folder_path = "data/raw"
output_file = "data/output/molina_all_agents.xlsx"
# ───────────────────────────────────────────────────────

def process_molina_file(filepath):
    """Process a single Molina CSV and return agent member counts."""
    df = pd.read_csv(filepath, encoding='latin1')
    
    # Keep active statuses only
    df_active = df[df["Status"].isin(["Active", "Pending Payment", "Pending Binder"])]
    
    # Sort so highest Member_Count stays on top
    df_sorted = df_active.sort_values("Member_Count", ascending=False)
    
    # Split single vs multi-member households
    df_multi = df_sorted[df_sorted["Member_Count"] > 1]
    df_single = df_sorted[df_sorted["Member_Count"] == 1]
    
    # Deduplicate only multi-member households by address
    df_multi_deduped = df_multi.drop_duplicates(subset=["Address1"])
    
    # Rejoin both groups
    df_deduped = pd.concat([df_multi_deduped, df_single])
    
    # Sum per agent
    agent_counts = (
        df_deduped
        .groupby(["Broker_Last_Name", "Broker_First_Name"])["Member_Count"]
        .sum()
        .reset_index()
    )
    
    return agent_counts

def main():
    results = []
    
    # Loop through all CSV files in data/raw/
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(folder_path, filename)
            print(f"Processing {filename}...")
            try:
                agent_counts = process_molina_file(filepath)
                results.append(agent_counts)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    if not results:
        print("No CSV files found in data/raw/")
        return
    
    # Combine all agents into one table
    final = (
        pd.concat(results)
        .groupby(["Broker_Last_Name", "Broker_First_Name"])["Member_Count"]
        .sum()
        .reset_index()
        .sort_values("Member_Count", ascending=False)
    )
    
    # Print results
    print("\n=== MOLINA ACTIVE MEMBERS PER AGENT ===\n")
    for _, row in final.iterrows():
        print(f"{row['Broker_First_Name']} {row['Broker_Last_Name']}: {row['Member_Count']} active members")
    
    print(f"\nTotal across all agents: {final['Member_Count'].sum()}")
    
    # Save to Excel
    final.to_excel(output_file, index=False)
    print(f"\nSaved to {output_file}")

if __name__ == "__main__":
    main()