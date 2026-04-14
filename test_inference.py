import requests
import pandas as pd
import json


url = "http://localhost:5001/invocations"



data = {
    "dataframe_split": {
        "columns": ["avg_price"],
        "data": [
            [50.0],   
            [100.0],  
            [500.0]   
        ]
    }
}

headers = {"Content-Type": "application/json"}

print(f"Sending request to model: {url}")
try:
    response = requests.post(url, json=data, headers=headers)
    
    if response.status_code == 200:
        print("\n>>> SUCCESS! Model response (Sales forecast):")
        print(response.json())
    else:
        print(f"\n>>> API ERROR: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"\n>>> Connection error: {e}")
    print("Is 'mlflow-serving' container definitely running?")