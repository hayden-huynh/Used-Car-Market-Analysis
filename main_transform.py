from datetime import datetime
from dotenv import load_dotenv
import curl_util
import minio_util
import pandas as pd


load_dotenv()
curl_session = curl_util.get_curl_session()


def transform():
    source = "cargurus"
    time_frame = datetime.now().strftime("%Y-%m-%d/%H")

    records = minio_util.download_json(source=source, time_frame=time_frame)

    df = pd.json_normalize(records)

    # Filter out records without one or more of: VIN, price, make, model, year, mileage
    df = df[
        df[
            [
                "specs.vin",
                "priceInfo.price",
                "priceInfo.expectedPrice",
                "specs.make",
                "specs.model",
                "specs.year",
                "specs.mileage",
            ]
        ]
        .notnull()
        .all(axis=1)
    ]

    # Enrich the data
    ## Add actual vs expected price difference percentage
    df["priceInfo.priceDiffPercent"] = (
        df["priceInfo.expectedPrice"] - df["priceInfo.price"]
    ) / df["priceInfo.expectedPrice"]
    df["priceInfo.priceDiffPercent"] = df["priceInfo.priceDiffPercent"].apply(
        lambda x: round(x * 100, 2)
    )

    ## Add mileage per year
    df["specs.mileagePerYear"] = df.apply(
        lambda row: (
            round(row["specs.mileage"] / (datetime.now().year - row["specs.year"]), 2)
            if (datetime.now().year - row["specs.year"]) != 0
            else row["specs.mileage"]
        ),
        axis=1,
    )

    # Cast certain numeric columns to integer
    integer_cols = [
        "seller.reviewCount",
        "history.daysAtDealer",
        "history.daysOnCarGurus",
        "history.accidentCount",
        "history.ownerCount",
        "priceInfo.savedCount",
    ]
    for col in integer_cols:
        df[col] = df[col].astype("Int64")

    # Upload final transformed data into MinIO as one combined CSV
    minio_util.upload_csv(df=df, source=source, time_frame=time_frame)


if __name__ == "__main__":
    transform()
