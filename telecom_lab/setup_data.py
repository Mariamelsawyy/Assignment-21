import pandas as pd
import numpy as np
from pathlib import Path

Path("include").mkdir(exist_ok=True)

recs = 1_000_000

# 1. Call Logs
pd.DataFrame({
    'user_id': np.random.randint(100, 500, size=recs),
    'call_type_id': np.random.choice([1, 2, 3], size=recs),
    'duration_mins': np.random.uniform(1, 45, size=recs)
}).to_csv("include/call_logs.csv", index=False)

# 2. Users
pd.DataFrame({
    'user_id': np.arange(100, 500),
    'region': np.random.choice(
        ['Cairo', 'Alex', 'Giza', 'Aswan'],
        size=400
    )
}).to_csv("include/users.csv", index=False)

# 3. Rates
pd.DataFrame({
    'call_type_id': [1, 2, 3],
    'call_type_name': ['Local', 'International', 'Roaming'],
    'rate_per_min': [0.5, 5.0, 12.0]
}).to_csv("include/rates.csv", index=False)

print("Data created inside include folder")