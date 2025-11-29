import curl_util
from datetime import datetime
import minio_util
import asyncio
import queue
import traceback


def request_listings_api(session, zip: str, distance: str, page: int):
    # Customizable parameters: zip, distance, pageNumber
    api_url = f"https://www.cargurus.com/Cars/searchPage.action?zip={zip}&distance={distance}&sourceContext=carGurusHomePageModel&sortDir=ASC&sortType=BEST_MATCH&srpVariation=DEFAULT_SEARCH&isDeliveryEnabled=true&nonShippableBaseline=0&pageNumber={page}&filtersModified=true"
    try:
        response = session.get(api_url)
        data = response.json()
        car_ids = []
        for listing in data["tiles"]:
            if "MERCH" not in listing["type"]:
                car_ids.append(str(listing["data"]["id"]))
        return car_ids
    except Exception as e:
        print(f"Caught Error: {e}")
        traceback.print_exc()


def request_details_api(
    session, id: str, zip: str, distance: str, retry_q: queue.Queue = None
):
    # Customizable parameters: inventoryListing (car id), searchZip, searchDistance
    api_url = f"https://www.cargurus.com/Cars/detailListingJson.action?inventoryListing={id}&searchZip={zip}&searchDistance={distance}&inclusionType=DEFAULT&pid=null&sourceContext=carGurusHomePageModel&isDAVE=false"
    try:
        response = session.get(api_url)
    except Exception as e:
        print(f"Caught Error: {e}")
        traceback.print_exc()
        if retry_q is not None:
            retry_q.put(id)
            print(f"Added id {id} into retry queue")
        return {}

    if response.status_code == 200:
        data = response.json()
        listing = data["listing"]
        seller = data["seller"]
        return {
            "listingId": listing.get("id", None),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "priceInfo": {
                "price": listing.get("price", None),
                "expectedPrice": listing.get("expectedPrice", None),
                "dealRating": listing.get("dealRatingKey", None),
                "savedCount": listing.get("savedCount", None),
            },
            "specs": {
                "vin": listing.get("vin", None),
                "fullName": listing.get("listingTitleOnly", None),
                "url": f"https://www.cargurus.com/Cars/inventorylisting/viewDetailsFilterViewInventoryListing.action?sourceContext=carGurusHomePageModel&entitySelectingHelper.selectedEntity=&zip={zip}#listing={id}",
                "make": listing.get("makeName", None),
                "model": listing.get("modelName", None),
                "year": listing.get("year", None),
                "trimName": listing.get("trimName", None),
                "mileage": listing.get("mileage", None),
                "condition": listing.get("vehicleCondition", None),
                "bodyType": (
                    data.get("autoEntityInfo").get("bodyStyle")
                    if "bodyStyle" in data.get("autoEntityInfo")
                    else None
                ),
                "exteriorColor": listing.get("localizedExteriorColor", None),
                "interiorColor": listing.get("localizedInteriorColor", None),
                "engine": listing.get("localizedEngineDisplayName", None),
                "driveTrain": listing.get("localizedDriveTrain", None),
                "transmission": listing.get("localizedTransmission", None),
                "mpgCity": (
                    listing.get("cityFuelEconomy").get("value")
                    if "cityFuelEconomy" in listing
                    else None
                ),
                "mpgHighway": (
                    listing.get("highwayFuelEconomy").get("value")
                    if "highwayFuelEconomy" in listing
                    else None
                ),
                "mpgCombined": (
                    listing.get("combinedFuelEconomy").get("value")
                    if "combinedFuelEconomy" in listing
                    else None
                ),
                "fuelType": listing.get("localizedFuelType", None),
                "options": listing.get("options", None),
            },
            "history": {
                "daysAtDealer": listing.get("listingHistory").get("daysAtDealer", None),
                "daysOnCarGurus": listing.get("listingHistory").get(
                    "daysOnCarGurus", None
                ),
                "accidentCount": (
                    listing.get("vehicleHistory").get("accidentCount")
                    if "accidentCount" in listing.get("vehicleHistory")
                    else None
                ),
                "ownerCount": (
                    listing.get("vehicleHistory").get("ownerCount")
                    if "ownerCount" in listing.get("vehicleHistory")
                    else None
                ),
                "hasVehicleHistoryReport": (
                    listing.get("vehicleHistory").get("hasVehicleHistoryReport")
                    if "hasVehicleHistoryReport" in listing.get("vehicleHistory")
                    else None
                ),
                "hasThirdPartyVehicleDamageData": (
                    listing.get("vehicleHistory").get("hasThirdPartyVehicleDamageData")
                    if "hasThirdPartyVehicleDamageData" in listing.get("vehicleHistory")
                    else None
                ),
                "isFleetVehicle": (
                    listing.get("vehicleHistory").get("isFleet")
                    if "isFleet" in listing.get("vehicleHistory")
                    else False
                ),
            },
            "seller": {
                "sellerId": seller.get("listingSellerId", None),
                "sellerType": seller.get("sellerType", None),
                "name": seller.get("name", None),
                "streetAddress": (
                    seller.get("address").get("street")
                    if "address" in seller and "street" in seller.get("address")
                    else None
                ),
                "city": (
                    seller.get("address").get("cityRegion")
                    if "address" in seller and "cityRegion" in seller.get("address")
                    else None
                ),
                "postalCode": (
                    seller.get("address").get("postalCode")
                    if "address" in seller and "postalCode" in seller.get("address")
                    else None
                ),
                "phoneNumber": seller.get("phoneNumber", None),
                "isFranchiseDealer": seller.get("isFranchiseDealer", False),
                "avgRating": seller.get("averageRating", None),
                "reviewCount": seller.get("reviewCount", None),
            },
        }
    else:
        print(response.status_code)
        return {}


async def extract():
    zip = "75081"
    distance = "50"
    session = curl_util.get_curl_session()
    retry_queue = queue.Queue()
    unique_cars = set()
    for current_page in range(1, 10 + 1):
        # Fetch car ids from current search page
        car_ids = request_listings_api(session, zip, distance, current_page)

        # Extract and upload all car data on current page
        for id in car_ids:
            car_data = request_details_api(session, id, zip, distance, retry_queue)
            if car_data:
                unique_cars.add(car_data["specs"]["vin"])
                asyncio.create_task(
                    minio_util.upload_json(
                        source="cargurus",
                        car_vin=car_data["specs"]["vin"],
                        car_data=car_data,
                    )
                )

        # Retry failed car detail requests of the same page. Retry only once
        if not retry_queue.empty():
            while not retry_queue.empty():
                car_data = request_details_api(
                    session, retry_queue.get(), zip, distance
                )
                if not car_data:
                    continue
                else:
                    unique_cars.add(car_data["specs"]["vin"])
                    asyncio.create_task(
                        minio_util.upload_json(
                            source="cargurus",
                            car_vin=car_data["specs"]["vin"],
                            car_data=car_data,
                        )
                    )

        # Move to next page
        current_page += 1

    # Wait for remaining unfinished file uploads
    all_tasks = asyncio.all_tasks()
    main_coro = asyncio.current_task()
    all_tasks.remove(main_coro)
    await asyncio.wait(all_tasks)


def run_extract_sync():
    asyncio.run(extract())


if __name__ == "__main__":
    run_extract_sync()
