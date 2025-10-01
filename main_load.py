import os
import ast
import json
import psycopg
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from minio_util import download_csv


load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PWD = os.getenv("POSTGRES_PWD")


# def get_auto_transmission_speed(value):
#     if value:
#         try:
#             return int(value)
#         except (ValueError, TypeError):
#             return 0
#     else:
#         return None


def load():
    # Download the CSV file from MinIO into a DataFrame
    source = "cargurus"
    time_frame = datetime.now().strftime("%Y-%m-%d/%H")
    # test_time_frame = "2025-05-19/19"
    df = download_csv(source, time_frame)

    # Convert specs.options from string representation of list to actual list
    df["specs.options"] = df["specs.options"].apply(
        lambda x: ast.literal_eval(x) if pd.notna(x) else []
    )

    # Cast all columns to object type and replace NaN with None for PostgreSQL type adaptation
    df = df.astype(object).where(pd.notna(df), None)

    # Load the DataFrame into PostgreSQL
    db_params = {
        "dbname": "used_cars",
        "user": POSTGRES_USER,
        "password": POSTGRES_PWD,
        "host": "localhost",
        "port": 5432,
    }
    with psycopg.connect(**db_params) as conn:
        with conn.cursor() as cur:
            dim_car_cols = [
                "full_name",
                "page_url",
                "make",
                "model",
                "year_release",
                "trim_name",
                "mileage",
                "mileage_per_year",
                "condition",
                "body_type",
                "exterior_color",
                "interior_color",
                "engine",
                # "engine_full_name",
                # "cylinder_count",
                # "engine_size",
                # "engine_config",
                "fuel_type",
                # "fuel_grade",
                # "engine_hp",
                # "engine_torque",
                # "horsepower_rpm",
                # "torque_rpm",
                "drivetrain",
                "transmission",
                # "transmission_type",
                # "automatic_trans_type",
                # "automatic_trans_speed",
                "mpg_city",
                "mpg_highway",
                "mpg_combined",
                "options",
            ]
            dim_history_cols = [
                "days_at_dealer",
                "days_on_cargurus",
                "accident_count",
                "owner_count",
                "has_vehicle_history_report",
                "has_thirdparty_vehicle_damage_report",
                "is_fleet_vehicle",
                # "base_msrp",
                # "destination_charge",
            ]
            dim_seller_cols = [
                "seller_type",
                "seller_name",
                "street_address",
                "city",
                "postal_code",
                "phone_number",
                "is_franchise_dealer",
                "avg_rating",
                "review_count",
            ]

            for _, row in df.iterrows():
                # Insert car specs data
                # auto_trans_speed = get_auto_transmission_speed(
                #     row["specs.automaticTransSpeed"]
                # )
                cur.execute(
                    f"""
                    INSERT INTO dim_car (vin, {', '.join(dim_car_cols)})
                    VALUES ({', '.join(['%s'] * (len(dim_car_cols) + 1))})
                    ON CONFLICT (vin) DO UPDATE
                    SET {', '.join([f"{col} = EXCLUDED.{col}" for col in dim_car_cols])}
                    """,
                    [
                        row["specs.vin"],
                        row["specs.fullName"],
                        row["specs.url"],
                        row["specs.make"],
                        row["specs.model"],
                        row["specs.year"],
                        row["specs.trimName"],
                        row["specs.mileage"],
                        row["specs.mileagePerYear"],
                        row["specs.condition"],
                        row["specs.bodyType"],
                        row["specs.exteriorColor"],
                        row["specs.interiorColor"],
                        row["specs.engine"],
                        # row["specs.engineFullName"],
                        # row["specs.cylinderCount"],
                        # row["specs.engineSize"],
                        # row["specs.engineConfig"],
                        row["specs.fuelType"],
                        # row["specs.fuelGrade"],
                        # row["specs.engineHp"],
                        # row["specs.engineTorque"],
                        # row["specs.horsepowerRPM"],
                        # row["specs.torqueRPM"],
                        row["specs.driveTrain"],
                        row["specs.transmission"],
                        # row["specs.transmissionType"],
                        # row["specs.automaticTransType"],
                        # auto_trans_speed,
                        row["specs.mpgCity"],
                        row["specs.mpgHighway"],
                        row["specs.mpgCombined"],
                        row["specs.options"],
                    ],
                )

                # Insert car history data
                cur.execute(
                    f"""
                    INSERT INTO dim_history (vin, {', '.join(dim_history_cols)})
                    VALUES ({', '.join(['%s'] * (len(dim_history_cols) + 1))})
                    ON CONFLICT (vin) DO UPDATE
                    SET {', '.join([f"{col} = EXCLUDED.{col}" for col in dim_history_cols])}
                    """,
                    [
                        row["specs.vin"],
                        row["history.daysAtDealer"],
                        row["history.daysOnCarGurus"],
                        row["history.accidentCount"],
                        row["history.ownerCount"],
                        row["history.hasVehicleHistoryReport"],
                        row["history.hasThirdPartyVehicleDamageData"],
                        row["history.isFleetVehicle"],
                        # row["history.baseMsrp"],
                        # row["history.destinationCharge"],
                    ],
                )

                # Insert seller data
                cur.execute(
                    f"""
                    INSERT INTO dim_seller (seller_id, {', '.join(dim_seller_cols)})
                    VALUES ({', '.join(['%s'] * (len(dim_seller_cols) + 1))})
                    ON CONFLICT (seller_id) DO UPDATE
                    SET {', '.join([f"{col} = EXCLUDED.{col}" for col in dim_seller_cols])}
                    """,
                    [
                        row["seller.sellerId"],
                        row["seller.sellerType"],
                        row["seller.name"],
                        row["seller.streetAddress"],
                        row["seller.city"],
                        row["seller.postalCode"],
                        row["seller.phoneNumber"],
                        row["seller.isFranchiseDealer"],
                        (
                            round(row["seller.avgRating"], 2)
                            if row["seller.avgRating"]
                            else None
                        ),
                        row["seller.reviewCount"],
                    ],
                )

                # Insert car listing data
                cur.execute(
                    """
                    INSERT INTO fact_listing (listing_id, created_at, price, expected_price, price_diff_percent, deal_rating, save_count, vin, seller_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    [
                        row["listingId"],
                        row["timestamp"],
                        row["priceInfo.price"],
                        row["priceInfo.expectedPrice"],
                        row["priceInfo.priceDiffPercent"],
                        row["priceInfo.dealRating"],
                        row["priceInfo.savedCount"],
                        row["specs.vin"],
                        row["seller.sellerId"],
                    ],
                )

                # Insert new unique car option(s)
                for option in row["specs.options"]:
                    cur.execute(
                        """
                        INSERT INTO car_options (option_name)
                        VALUES (%s)
                        ON CONFLICT (option_name) DO NOTHING
                        """,
                        [option],
                    )


if __name__ == "__main__":
    load()
