import json
from pathlib import Path
import pandas as pd


ROOT_DIR = Path(__file__).parent


def get_data():
    with open(ROOT_DIR / "regions.json", "r") as file:
        data = json.load(file)
    return data


def to_excel(data: dict):
    df = pd.DataFrame()
    df["region"] = [rn for rn in data.values()]
    df["index"] = [i for i in data.keys()]
    df.to_excel(ROOT_DIR / "regions.xlsx", index=False)


if __name__ == "__main__":
    to_excel(get_data())
