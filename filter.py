import pandas as pd
from datetime import datetime, timedelta

# Load the CSV file without headers
file_path = 'output.csv'  # Replace with your actual file path
data = pd.read_csv(file_path, header=None)

# Assign column indices to fields (adjust if needed)
COMPANY_COL = 0  # Company name
DATETIME_COL = 5  # Datetime field

# Parse the datetime column
data[DATETIME_COL] = pd.to_datetime(data[DATETIME_COL])

# Step 1: Count occurrences of each company
company_counts = data[COMPANY_COL].value_counts()

# ------------------- LOGIC FOR real_jobs_with_counts.csv -------------------
# Filter real jobs (companies mentioned less than 3 times)
real_companies = company_counts[company_counts <= 5].index
real_jobs = data[data[COMPANY_COL].isin(real_companies)].copy()

# Add the company occurrence count as a new column
real_jobs['count'] = real_jobs[COMPANY_COL].map(company_counts)

# Load the Forbes 2000 dataset
forbes_file_path = 'filter_files/forbes2000.csv'
forbes_data = pd.read_csv(forbes_file_path, header=None, names=[
    'rownames', 'rank', 'name', 'country', 'category', 'sales', 'profits', 'assets', 'marketvalue'
])

# Ensure Forbes 2000 company names are strings
forbes_data['name'] = forbes_data['name'].astype(str)

# Function for loose matching
def partial_match(company, companies):
    if not isinstance(company, str):
        return False
    return any(company.lower() in name.lower() for name in companies)

# Filter Forbes 2000 companies and add them to real_jobs
forbes_companies = forbes_data['name']
is_forbes_company = data[COMPANY_COL].apply(lambda x: partial_match(x, forbes_companies))
forbes_real_jobs = data[is_forbes_company].copy()

# Add the count column for Forbes companies
forbes_real_jobs['count'] = forbes_real_jobs[COMPANY_COL].map(company_counts)

# Combine real_jobs with Forbes 2000 jobs, avoiding duplicates
combined_real_jobs = pd.concat([real_jobs, forbes_real_jobs]).drop_duplicates()

# Reorder columns to place count first
combined_real_jobs = combined_real_jobs[['count', COMPANY_COL] + list(combined_real_jobs.columns.drop(['count', COMPANY_COL]))]

# Save the final real jobs file with Forbes 2000 companies included
open('real_jobs_with_counts.csv', 'w').close()
combined_real_jobs.to_csv('real_jobs_with_counts.csv', index=False, header=False)

# Print the count of Forbes 2000 companies added
print(f"Number of Forbes 2000 companies added: {len(forbes_real_jobs)}")

# ------------------- LOGIC FOR real_jobs_with_h1b.csv -------------------
# Load the 2023.csv H1B dataset
h1b_file_path = 'filter_files/2023.csv'
h1b_data = pd.read_csv(h1b_file_path, header=None)

# Define column indices for 2023.csv (H1B dataset)
H1B_YEAR_COL = 0  # Fiscal Year
H1B_EMPLOYER_COL = 1  # Employer

# Ensure all values in the H1B dataset employer column are strings
h1b_data[H1B_EMPLOYER_COL] = h1b_data[H1B_EMPLOYER_COL].astype(str)

# Filter all real jobs (including Forbes 2000) for H1B sponsorship
h1b_employers = h1b_data[H1B_EMPLOYER_COL]
all_h1b_jobs = combined_real_jobs[
    combined_real_jobs[COMPANY_COL].apply(lambda x: partial_match(x, h1b_employers))
]

# Clean the file and save
open('real_jobs_with_h1b.csv', 'w').close()
all_h1b_jobs.to_csv('real_jobs_with_h1b.csv', index=False, header=False)

# Print the count of real jobs that sponsor H1B
print(f"Number of real jobs (including Forbes 2000) that sponsor H1B: {len(all_h1b_jobs)}")

# ------------------- LOGIC FOR recent_real_jobs_with_counts.csv -------------------
# Filter for recent real jobs applied within the last 48 hours from the concatenated dataset
now = datetime.now()
time_threshold = now - timedelta(hours=24)
recent_combined_real_jobs = combined_real_jobs[combined_real_jobs[DATETIME_COL] >= time_threshold]

# Clean the file and save
open('recent_real_jobs_with_counts.csv', 'w').close()
recent_combined_real_jobs.to_csv('recent_real_jobs_with_counts.csv', index=False, header=False)

# Print the count of recent real jobs saved
print(f"Number of recent real jobs saved: {len(recent_combined_real_jobs)}")

# ------------------- LOGIC FOR recent_real_jobs_with_h1b.csv -------------------
# Filter recent real jobs (last 48 hours) for H1B sponsorship
recent_h1b_jobs = recent_combined_real_jobs[
    recent_combined_real_jobs[COMPANY_COL].apply(lambda x: partial_match(x, h1b_employers))
]

# Clean the file and save
open('recent_real_jobs_with_h1b.csv', 'w').close()
recent_h1b_jobs.to_csv('recent_real_jobs_with_h1b.csv', index=False, header=False)

# Print the count of H1B sponsored jobs
print(f"Number of H1B sponsored recent real jobs saved: {len(recent_h1b_jobs)}")


# # ------------------- PRINT FINAL OUTPUT -------------------
# print(f"Filtered companies from output.csv matching 2023.csv (partial match) saved in '2023_full_filtered.csv'.")
# print("Filtered files created:")
# print("1. Real jobs with counts saved in 'real_jobs_with_counts.csv'")
# print("2. Recent real jobs with counts saved in 'recent_real_jobs_with_counts.csv'")
# print("3. Recent fake jobs with counts saved in 'recent_fake_jobs_with_counts.csv'")
# print(f"4. H1B matching real jobs saved in 'recent_real_jobs_with_h1b.csv'.")









# # ------------------- LOGIC FOR recent_fake_jobs_with_counts.csv -------------------
# # Filter recent fake jobs (companies mentioned more than 5 times in last 48 hours)
# fake_companies = company_counts[company_counts >= 1].index
# recent_fake_jobs = data[(data[COMPANY_COL].isin(fake_companies)) & (data[DATETIME_COL] >= time_threshold)].copy()

# # Add the company occurrence count as a new column
# recent_fake_jobs['count'] = recent_fake_jobs[COMPANY_COL].map(company_counts)

# # Reorder columns to place count first
# recent_fake_jobs = recent_fake_jobs[['count', COMPANY_COL] + list(recent_fake_jobs.columns.drop(['count', COMPANY_COL]))]

# # Clean the file and save
# open('recent_fake_jobs_with_counts.csv', 'w').close()
# recent_fake_jobs.to_csv('recent_fake_jobs_with_counts.csv', index=False, header=False)