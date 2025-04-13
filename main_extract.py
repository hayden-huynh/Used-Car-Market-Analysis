from curl_cffi import requests
from dotenv import load_dotenv
from datetime import datetime
import minio_util
import asyncio
import random
import queue
import os


load_dotenv()
smartproxy_endpoint = os.getenv("STICKY_SMARTPROXY")

smartproxy_proxies = {
    "http": smartproxy_endpoint,
    "https": smartproxy_endpoint,
}


# def send_curl_request(url):
#     browser = random.choice(["chrome", "edge", "safari", "firefox"])
#     resp = requests.get(
#         url,
#         impersonate=browser,
#         proxies=smartproxy_proxies,
#         verify=False,
#     )
#     return resp


def get_curl_session():
    browser = random.choice(["chrome", "edge", "safari", "firefox"])
    session = requests.Session(
        impersonate="chrome",
        proxies=smartproxy_proxies,
        verify=False,
    )
    return session


def request_listings_api(session, zip: str, distance: str, page: int):
    # Customizable parameters: zip, distance, pageNumber
    api_url = f"https://www.cargurus.com/Cars/searchPage.action?zip={zip}&distance={distance}&sourceContext=carGurusHomePageModel&inventorySearchWidgetType=AUTO&sortDir=ASC&sortType=BEST_MATCH&srpVariation=DEFAULT_SEARCH&isDeliveryEnabled=true&nonShippableBaseline=0&pageNumber={page}&filtersModified=true"
    try:
        response = session.get(api_url)
    except Exception as e:
        print(f"Caught Error: {e}")
    data = response.json()
    car_ids = (
        [
            str(listing["data"]["id"])
            for listing in data["tiles"]
            if listing["type"] != "MERCH"
        ]
        if "tiles" in data
        else []
    )
    return car_ids


def request_details_api(
    session, id: str, zip: str, distance: str, retry_q: queue.Queue = None
):
    # Customizable parameters: inventoryListing (car id), searchZip, searchDistance
    api_url = f"https://www.cargurus.com/Cars/detailListingJson.action?inventoryListing={id}&searchZip={zip}&searchDistance={distance}&inclusionType=DEFAULT&pid=null&sourceContext=carGurusHomePageModel&isDAVE=false"
    try:
        response = session.get(api_url)
    except Exception as e:
        print(f"Caught Error: {e}")
        if retry_q is not None:
            retry_q.put(id)
            print(f"Added id {id} into retry queue")
        return {}

    if response.status_code == 200:
        data = response.json()
        info = data["listing"]
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H-%M-%S"),
            "vin": info.get("vin", None),
            "priceInfo": {
                "price": info.get("price", None),
                "priceString": info.get("priceString", None),
                "dealRating": info.get("dealRatingKey", None),
                "savedCount": info.get("savedCount", None),
            },
            "specs": {
                "fullName": info.get("listingTitleOnly", None),
                "url": f"https://www.cargurus.com/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?sourceContext=carGurusHomePageModel&entitySelectingHelper.selectedEntity=&zip={zip}#listing={id}",
                "stockNumber": info.get("stockNumber", None),
                "make": info.get("makeName", None),
                "model": info.get("modelName", None),
                "year": info.get("year", None),
                "trimName": info.get("trimName", None),
                "mileage": info.get("mileage", None),
                "bodyType": (
                    data.get("autoEntityInfo").get("bodyStyle")
                    if "bodyStyle" in data.get("autoEntityInfo")
                    else None
                ),
                "exteriorColor": info.get("localizedExteriorColor", None),
                "interiorColor": info.get("localizedInteriorColor", None),
                "driveTrain": info.get("localizedDriveTrain", None),
                "transmission": info.get("localizedTransmission", None),
                "mpgCity": (
                    info.get("cityFuelEconomy").get("value")
                    if "cityFuelEconomy" in info
                    else None
                ),
                "mpgHighway": (
                    info.get("highwayFuelEconomy").get("value")
                    if "highwayFuelEconomy" in info
                    else None
                ),
                "mpgCombined": (
                    info.get("combinedFuelEconomy").get("value")
                    if "combinedFuelEconomy" in info
                    else None
                ),
                "fuelType": info.get("localizedFuelType", None),
                "options": info.get("options", None),
                "daysAtDealer": info.get("listingHistory").get("daysAtDealer", None),
                "daysOnCarGurus": info.get("listingHistory").get(
                    "daysOnCarGurus", None
                ),
                "accidentCount": (
                    info.get("vehicleHistory").get("accidentCount")
                    if "accidentCount" in info.get("vehicleHistory")
                    else None
                ),
                "ownerCount": (
                    info.get("vehicleHistory").get("ownerCount")
                    if "ownerCount" in info.get("vehicleHistory")
                    else None
                ),
            },
        }
    else:
        print(response.status_code)
        return {}


async def main():
    zip = "75081"
    distance = "50"
    session = get_curl_session()
    retry_queue = queue.Queue()
    unique_cars = set()
    for current_page in range(1, 31):
        # Fetch car ids from current search page
        car_ids = request_listings_api(session, zip, distance, current_page)

        # Extract and upload all car data on current page
        print(
            f"=============================== Page {current_page} ====================================="
        )
        for id in car_ids:
            car_data = request_details_api(session, id, zip, distance, retry_queue)
            if car_data:
                unique_cars.add(car_data["vin"])
                print(car_data["specs"]["fullName"])
                asyncio.create_task(
                    minio_util.upload_json(source="cargurus", car_data=car_data)
                )
                # await minio_util.upload_json(source="cargurus", car_data=car_data)

        # Retry failed car detail requests of the same page. Retry only once
        if not retry_queue.empty():
            print(
                f"=============================== Page {current_page} Retries ====================================="
            )
            while not retry_queue.empty():
                car_data = request_details_api(
                    session, retry_queue.get(), zip, distance
                )
                if not car_data:
                    print("Car Not Found on Retry. Car Skipped")
                else:
                    unique_cars.add(car_data["vin"])
                    print(car_data["specs"]["fullName"])
                    asyncio.create_task(
                        minio_util.upload_json(source="cargurus", car_data=car_data)
                    )
                    # await minio_util.upload_json(source="cargurus", car_data=car_data)

        # await asyncio.gather(*upload_tasks)
        # upload_tasks = []

        # Move to next page
        current_page += 1

    # Wait for remaining unfinished file uploads
    all_tasks = asyncio.all_tasks()
    main_coro = asyncio.current_task()
    all_tasks.remove(main_coro)
    await asyncio.wait(all_tasks)

    print(f"\n\nSuccessfully extracted {len(unique_cars)} car records")


if __name__ == "__main__":
    asyncio.run(main())
