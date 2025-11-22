from datetime import datetime
from dotenv import load_dotenv
import curl_util
import minio_util
import pandas as pd
import os


load_dotenv()
# AUTODEV_KEY = os.getenv("AUTODEV_KEY")
curl_session = curl_util.get_curl_session()


# def enrich_car_data(row):
#     vin = row["specs.vin"]
#     # print(f"Enriching data for VIN: {vin}")
#     mpgCity = row["specs.mpgCity"]
#     mpgHighway = row["specs.mpgHighway"]
#     mpgCombined = row["specs.mpgCombined"]
#     index = [
#         "specs.engineFullName",
#         "specs.cylinderCount",
#         "specs.engineSize",
#         "specs.engineConfig",
#         "specs.fuelGrade",
#         "specs.engineHp",
#         "specs.engineTorque",
#         "specs.horsepowerRPM",
#         "specs.torqueRPM",
#         "specs.transmissionType",
#         "specs.automaticTransType",
#         "specs.automaticTransSpeed",
#         "history.baseMsrp",
#         "history.destinationCharge",
#         "specs.mpgCity",
#         "specs.mpgHighway",
#         "specs.mpgCombined",
#     ]

#     url = f"https://auto.dev/api/vin/{vin}?apikey={AUTODEV_KEY}"
#     try:
#         response = curl_session.get(url)
#         if response.status_code == 200:
#             # print(f"----- Successful")
#             data = response.json()
#             _mpgCity = data.get("mpg", {}).get("city", None)
#             _mpgHighway = data.get("mpg", {}).get("highway", None)
#             _mpgCity = float(_mpgCity) if _mpgCity else None
#             _mpgHighway = float(_mpgHighway) if _mpgHighway else None
#             # added_data = pd.Series(
#             #     {
#             #         "specs.engineFullName": data.get("engine", {}).get("name", None),
#             #         "specs.cylinderCount": data.get("engine", {}).get("cylinder", None),
#             #         "specs.engineSize": data.get("engine", {}).get("size", None),
#             #         "specs.engineConfig": data.get("engine", {}).get(
#             #             "configuration", None
#             #         ),
#             #         "specs.fuelGrade": data.get("engine", {}).get("fuelType", None),
#             #         "specs.engineHp": data.get("engine", {}).get("horsepower", None),
#             #         "specs.engineTorque": data.get("engine", {}).get("torque", None),
#             #         "specs.horsepowerRPM": data.get("engine", {})
#             #         .get("rpm", {})
#             #         .get("horsepower", None),
#             #         "specs.torqueRPM": data.get("engine", {})
#             #         .get("rpm", {})
#             #         .get("torque", None),
#             #         "specs.transmissionType": data.get("transmission", {}).get(
#             #             "transmissionType", None
#             #         ),
#             #         "specs.automaticTransType": data.get("transmission", {}).get(
#             #             "automaticType", None
#             #         ),
#             #         "specs.automaticTransSpeed": data.get("transmission", {}).get(
#             #             "numberOfSpeeds", None
#             #         ),
#             #         "history.baseMsrp": data.get("price", {}).get("baseMsrp", None),
#             #         "history.destinationCharge": data.get("price", {}).get(
#             #             "deliveryCharges", None
#             #         ),
#             #         "specs.mpgCity": _mpgCity if not mpgCity else mpgCity,
#             #         "specs.mpgHighway": _mpgHighway if not mpgHighway else mpgHighway,
#             #         "specs.mpgCombined": (
#             #             (
#             #                 0.55 * _mpgCity + 0.45 * _mpgHighway
#             #                 if _mpgCity and _mpgHighway
#             #                 else None
#             #             )
#             #             if not mpgCombined
#             #             else mpgCombined
#             #         ),
#             #     },
#             #     index=index,
#             # )
#             added_data = [
#                 data.get("engine", {}).get("name", None),
#                 data.get("engine", {}).get("cylinder", None),
#                 data.get("engine", {}).get("size", None),
#                 data.get("engine", {}).get("configuration", None),
#                 data.get("engine", {}).get("fuelType", None),
#                 data.get("engine", {}).get("horsepower", None),
#                 data.get("engine", {}).get("torque", None),
#                 data.get("engine", {}).get("rpm", {}).get("horsepower", None),
#                 data.get("engine", {}).get("rpm", {}).get("torque", None),
#                 data.get("transmission", {}).get("transmissionType", None),
#                 data.get("transmission", {}).get("automaticType", None),
#                 data.get("transmission", {}).get("numberOfSpeeds", None),
#                 data.get("price", {}).get("baseMsrp", None),
#                 data.get("price", {}).get("deliveryCharges", None),
#                 _mpgCity if not mpgCity else mpgCity,
#                 _mpgHighway if not mpgHighway else mpgHighway,
#                 (
#                     (
#                         (0.55 * _mpgCity + 0.45 * _mpgHighway)
#                         if _mpgCity and _mpgHighway
#                         else None
#                     )
#                     if not mpgCombined
#                     else mpgCombined
#                 ),
#             ]
#             # print(
#             #     "==================================================================================="
#             # )
#             # print(added_data)
#             return added_data
#         else:
#             print(f"Failed to enrich data for VIN: {vin}")
#             # return pd.Series([None] * (len(index) - 3), index=index[:-3])
#             return [None] * (len(index) - 3) + [mpgCity, mpgHighway, mpgCombined]
#     except Exception as e:
#         print(f"Caught error while enriching {vin}: {e}")
#         # return pd.Series([None] * (len(index) - 3), index=index[:-3])
#         return [None] * (len(index) - 3) + [mpgCity, mpgHighway, mpgCombined]


def transform():
    source = "cargurus"
    time_frame = datetime.now().strftime("%Y-%m-%d/%H")
    # test_time_frame = "2025-05-22/13"

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
    # df["specs.mileagePerYear"] = (
    #     df["specs.mileage"] / (datetime.now().year - df["specs.year"])
    # ).apply(lambda x: round(x, 2))
    df["specs.mileagePerYear"] = df.apply(
        lambda row: (
            round(row["specs.mileage"] / (datetime.now().year - row["specs.year"]), 2)
            if (datetime.now().year - row["specs.year"]) != 0
            else row["specs.mileage"]
        ),
        axis=1,
    )

    ## Add extra engine, transmission, msrp, mpg data from API
    # df[
    #     [
    #         "specs.engineFullName",
    #         "specs.cylinderCount",
    #         "specs.engineSize",
    #         "specs.engineConfig",
    #         "specs.fuelGrade",
    #         "specs.engineHp",
    #         "specs.engineTorque",
    #         "specs.horsepowerRPM",
    #         "specs.torqueRPM",
    #         "specs.transmissionType",
    #         "specs.automaticTransType",
    #         "specs.automaticTransSpeed",
    #         "history.baseMsrp",
    #         "history.destinationCharge",
    #         "specs.mpgCity",
    #         "specs.mpgHighway",
    #         "specs.mpgCombined",
    #     ]
    # ] = df.apply(
    #     lambda row: enrich_car_data(row),
    #     axis=1,
    #     result_type="expand",
    # )

    # Cast certain numeric columns to integer
    integer_cols = [
        # "specs.cylinderCount",
        # "specs.engineHp",
        # "specs.engineTorque",
        # "specs.horsepowerRPM",
        # "specs.torqueRPM",
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
