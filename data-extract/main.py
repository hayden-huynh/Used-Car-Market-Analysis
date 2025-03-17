from curl_cffi import requests
from dotenv import load_dotenv
from rich import print
from rich.json import JSON
import json
from time import sleep
import os


load_dotenv()
proxy_key = os.getenv("SCRAPEOPS_API_KEY")

proxies = {
    "http": f"http://scrapeops.country=us:{proxy_key}@residential-proxy.scrapeops.io:8181",
    "https": f"http://scrapeops.country=us:{proxy_key}@residential-proxy.scrapeops.io:8181",
}


def get_session():
    session = requests.Session(impersonate="chrome", proxies=proxies)
    return session


def request_listings_api(session: requests.Session, zip: str, distance: str, page: int):
    # Customizable parameters: zip, distance, pageNumber
    api_url = f"https://www.cargurus.com/Cars/searchPage.action?zip={zip}&distance={distance}&sourceContext=carGurusHomePageModel&inventorySearchWidgetType=AUTO&sortDir=ASC&sortType=BEST_MATCH&srpVariation=DEFAULT_SEARCH&isDeliveryEnabled=true&nonShippableBaseline=0&pageNumber={page}&filtersModified=true"
    response = session.get(api_url)
    response.raise_for_status()
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


def request_details_api(session: requests.Session, id: str, zip: str, distance: str):
    # Customizable parameters: inventoryListing (car id), searchZip, searchDistance
    api_url = f"https://www.cargurus.com/Cars/detailListingJson.action?inventoryListing={id}&searchZip={zip}&searchDistance={distance}&inclusionType=DEFAULT&pid=null&sourceContext=carGurusHomePageModel&isDAVE=false"
    response = session.get(api_url)
    response.raise_for_status()
    data = response.json()
    info = data["listing"]
    return {
        "priceInfo": {
            "price": info["price"],
            "priceString": info["priceString"],
            "dealRating": info["dealRatingKey"],
            "savedCount": info["savedCount"],
        },
        "specs": {
            "url": f"https://www.cargurus.com/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?sourceContext=carGurusHomePageModel&entitySelectingHelper.selectedEntity=&zip={zip}#listing={id}",
            "vin": info["vin"],
            "stockNumber": info["stockNumber"],
            "make": info["makeName"],
            "model": info["modelName"],
            "year": info["year"],
            "trimName": info.get("trimName", ""),
            "mileage": info["mileage"],
            "bodyType": data["autoEntityInfo"]["bodyStyle"],
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
            "daysAtDealer": info["listingHistory"]["daysAtDealer"],
            "daysOnCarGurus": info["listingHistory"]["daysOnCarGurus"],
            "accidentCount": (
                info.get("vehicleHistory").get("accidentCount")
                if "accidentCount" in info.get("vehicleHistory")
                else ""
            ),
            "ownerCount": (
                info.get("vehicleHistory", "").get("ownerCount", "")
                if "ownerCount" in info.get("vehicleHistory")
                else ""
            ),
        },
    }


def main():
    zip = "75081"
    distance = "50"
    current_page = 1
    session = get_session()
    while car_ids := request_listings_api(session, zip, distance, current_page):
        print(
            f"=============================== Page {current_page} ====================================="
        )
        for id in car_ids:
            car_data = request_details_api(session, id, zip, distance)
            print(JSON(json.dumps(car_data)))
            sleep(0.5)
        current_page += 1


if __name__ == "__main__":
    main()
