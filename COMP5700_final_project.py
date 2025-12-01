import pandas as pd
import re
import os

# assign working directory to current script location
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

# =========== TASK 1 ============
print("Reading Task 1 dataframe 'all_pull_request'...")
df1 = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")

task1 = df1[["title", "id", "agent", "body", "repo_id", "repo_url"]].rename(
    columns={
        "title": "TITLE",
        "id": "ID",
        "agent": "AGENTNAME",
        "body": "BODYSTRING",
        "repo_id": "REPOID",
        "repo_url": "REPOURL"
    }
)

task1.to_csv("task1.csv", index=False)
print("Saved task1.csv.")

# ========== TASK 2 ============
print("Reading Task 2 dataframe `all_repository`...")
df2 = pd.read_parquet("hf://datasets/hao-li/AIDev/all_repository.parquet")

task2 = df2[["id", "language", "stars", "url"]].rename(
    columns={
        "id": "REPOID",
        "language": "LANG",
        "stars": "STARS",
        "url": "REPOURL"
    }
)

task2.to_csv("task2.csv", index=False)
print("Saved task2.csv.")

# =========== TASK 3 ============
print("Reading Task 3 dataframe `pr_task_type`...")
df3 = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_task_type.parquet")

task3 = df3[["id", "title", "reason", "type", "confidence"]].rename(
    columns={
        "id": "PRID",
        "title": "PRTITLE",
        "reason": "PRREASON",
        "type": "PRTYPE",
        "confidence": "CONFIDENCE",
    }
)

task3.to_csv("task3.csv", index=False)
print("Saved task3.csv.")

# ============== TASK 4 ==============
print("Reading Task 4 dataframe `pr_commit_details`...")
df4 = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")

def clean_patch(text):
    if not isinstance(text, str):
        return ""
    return re.sub(r"[^A-Za-z0-9 \t\n\r\+\-\=\:\;\,\.\(\)\{\}\[\]\/\\]", "", text)

print("Cleaning 'patch' of special characters...")
df4["patch_clean"] = df4["patch"].apply(clean_patch)

task4 = df4[
    ["pr_id", "sha", "message", "filename", "status", "additions", "deletions", "changes", "patch_clean"]
].rename(
    columns={
        "pr_id": "PRID",
        "sha": "PRSHA",
        "message": "PRCOMMITMESSAGE",
        "filename": "PRFILE",
        "status": "PRSTATUS",
        "additions": "PRADDS",
        "deletions": "PRDELSS",
        "changes": "PRCHANGECOUNT",
        "patch_clean": "PRDIFF"
    }
)

task4.to_csv("task4.csv", index=False)
print("Saved task4.csv.")

# ============= TASK 5 ================
print("Working on merging dataframes for Task 5...")
df5 = df1.merge(
    df3[["id", "type", "confidence"]],
    left_on="id",
    right_on="id",
    how="left"
)

security_terms = [
    "race", "racy", "buffer", "overflow", "stack", "integer", "signedness",
    "underflow", "improper", "unauthenticated", "gain access", "permission",
    "cross site", "css", "xss", "denial service", "dos", "crash", "deadlock",
    "injection", "request forgery", "csrf", "xsrf", "forged", "security",
    "vulnerability", "vulnerable", "exploit", "attack", "bypass", "backdoor",
    "threat", "expose", "breach", "violate", "fatal", "blacklist", "overrun",
    "insecure"
]

pattern = r"|".join(re.escape(term) for term in security_terms)

def detect_security(title, body):
    text = f"{title} {body}".lower()
    return 1 if re.search(pattern, text) else 0

print("Searching for security terms in title and body rows...")
df5["SECURITY"] = df5.apply(
    lambda row: detect_security(row.get("title", ""), row.get("body", "")),
    axis=1
)

task5 = df5.rename(
    columns={
        "id": "ID",
        "agent": "AGENT",
        "type": "TYPE",
        "confidence": "CONFIDENCE"
    }
)[["ID", "AGENT", "TYPE", "CONFIDENCE", "SECURITY"]]

task5.to_csv("task5.csv", index=False)
print("Saved to task5.csv.")