from datetime import datetime
import minio_util
import pandas as pd


def main():
    source = "cargurus"
    time_frame = datetime.now().strftime("%Y-%m-%d/%H")
    test_time_frame = "2025-04-12/22"

    records = minio_util.download_json(source=source, time_frame=test_time_frame)

    df = pd.json_normalize(records)

    print(
        df.sort_values("priceInfo.price")[["specs.fullName", "priceInfo.priceString"]]
    )


if __name__ == "__main__":
    main()
