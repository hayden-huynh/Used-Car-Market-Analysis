from curl_cffi import requests
from dotenv import load_dotenv
import random
import queue
import os


load_dotenv()
proxy_key = os.getenv("SCRAPEOPS_API_KEY")
smartproxy_endpoint = os.getenv("STICKY_SMARTPROXY")

scrapeops_proxies = {
    "http": f"http://scrapeops.country=us:{proxy_key}@residential-proxy.scrapeops.io:8181",
    "https": f"http://scrapeops.country=us:{proxy_key}@residential-proxy.scrapeops.io:8181",
}

smartproxy_proxies = {
    "http": smartproxy_endpoint,
    "https": smartproxy_endpoint,
}


def send_curl_request(url):
    browser = random.choice(["chrome", "edge", "safari", "firefox"])
    resp = requests.get(
        url,
        impersonate=browser,
        proxies=smartproxy_proxies,
        verify=False,
    )
    return resp


def get_curl_session():
    browser = random.choice(["chrome", "edge", "safari", "firefox"])
    session = requests.Session(
        impersonate="chrome",
        proxies=scrapeops_proxies,
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
            "vin": info.get("vin", ""),
            "priceInfo": {
                "price": info.get("price", ""),
                "priceString": info.get("priceString", ""),
                "dealRating": info.get("dealRatingKey", ""),
                "savedCount": info.get("savedCount", ""),
            },
            "specs": {
                "fullName": info.get("listingTitleOnly", ""),
                "url": f"https://www.cargurus.com/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?sourceContext=carGurusHomePageModel&entitySelectingHelper.selectedEntity=&zip={zip}#listing={id}",
                "stockNumber": info.get("stockNumber", ""),
                "make": info.get("makeName", ""),
                "model": info.get("modelName", ""),
                "year": info.get("year", ""),
                "trimName": info.get("trimName", ""),
                "mileage": info.get("mileage", ""),
                "bodyType": (
                    data.get("autoEntityInfo").get("bodyStyle")
                    if "bodyStyle" in data.get("autoEntityInfo")
                    else ""
                ),
                "exteriorColor": info.get("localizedExteriorColor", ""),
                "interiorColor": info.get("localizedInteriorColor", ""),
                "driveTrain": info.get("localizedDriveTrain", ""),
                "transmission": info.get("localizedTransmission", ""),
                "mpgCity": (
                    info.get("cityFuelEconomy").get("value")
                    if "cityFuelEconomy" in info
                    else ""
                ),
                "mpgHighway": (
                    info.get("highwayFuelEconomy").get("value")
                    if "highwayFuelEconomy" in info
                    else ""
                ),
                "mpgCombined": (
                    info.get("combinedFuelEconomy").get("value")
                    if "combinedFuelEconomy" in info
                    else ""
                ),
                "fuelType": info.get("localizedFuelType", ""),
                "options": info.get("options", ""),
                "daysAtDealer": info.get("listingHistory").get("daysAtDealer", ""),
                "daysOnCarGurus": info.get("listingHistory").get("daysOnCarGurus", ""),
                "accidentCount": (
                    info.get("vehicleHistory").get("accidentCount")
                    if "accidentCount" in info.get("vehicleHistory")
                    else ""
                ),
                "ownerCount": (
                    info.get("vehicleHistory").get("ownerCount")
                    if "ownerCount" in info.get("vehicleHistory")
                    else ""
                ),
            },
        }
    else:
        print(response.status_code)
        return {}


def main():
    zip = "75081"
    distance = "50"
    current_page = 1
    session = get_curl_session()
    retry_queue = queue.Queue()
    while car_ids := request_listings_api(session, zip, distance, current_page):
        # Extract data from all cars on current page
        print(
            f"=============================== Page {current_page} ====================================="
        )
        for id in car_ids:
            car_data = request_details_api(session, id, zip, distance, retry_queue)
            print("Car Not Found" if not car_data else car_data["specs"]["fullName"])
            # sleep(random.uniform(0.2, 1.5))

        # Retry failed car detail requests of the same page
        if not retry_queue.empty():
            print(
                f"=============================== Page {current_page} Retries ====================================="
            )
            while not retry_queue.empty():
                car_data = request_details_api(
                    session, retry_queue.get(), zip, distance
                )
                print(
                    "Car Not Found" if not car_data else car_data["specs"]["fullName"]
                )

        # Move to next page
        current_page += 1


if __name__ == "__main__":
    main()
