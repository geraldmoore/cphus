import json
from pathlib import Path

from shapely.geometry import shape

from src.cphus.core.config.logging import get_logger
from src.cphus.core.config.settings import get_settings
from src.cphus.crawl_firecrawl import BoligportalSearchParams, BoligsidenSearchParams, CrawlListings
from src.cphus.crud import ListingsManager
from src.cphus.discord import DiscordMessenger

settings = get_settings()

logger = get_logger(__name__)


def get_geometry_str(geometry: dict) -> str:
    return "|".join([f"{lon},{lat}" for lon, lat in geometry["coordinates"][0]])


def get_geometry_bounds_str(geometry: dict) -> str:
    return ",".join(str(bound) for bound in shape(geometry).bounds)


async def search_and_send_listings(
    manager: ListingsManager,
    messenger: DiscordMessenger,
    base_url: str,
    search_params: BoligsidenSearchParams | BoligportalSearchParams,
):
    # Get listings
    crawler = CrawlListings(base_url=base_url, search_params=search_params)
    all_listings = await crawler.scrape_listings(pages=1)

    # Find only new listings
    new_listings, _ = manager.find_new_listings(all_listings)
    new_count = len(new_listings)

    if new_count == 0:
        logger.info("No new listings found!")
        return

    else:
        logger.info(f"Found {new_count} new listings!")
        for count, listing in enumerate(new_listings.iter_rows(named=True)):
            listing_url = listing.get("listing_url")
            await messenger.send_message(listing_url)
            logger.info(f"Sent listing {count + 1} of {new_count}.")

    # Add the listings to the database
    logger.info("Messages sent.")
    _, _ = manager.add_new_listings(new_listings)

    logger.info("Listings added to the database.")


async def process_boligsiden(manager: ListingsManager, messenger: DiscordMessenger):
    # Load geometry
    with open("data/map.geojson") as f:
        geometry = json.load(f)["features"][0]["geometry"]

    # Configure search parameters
    search_params = BoligsidenSearchParams(
        mapBounds=get_geometry_bounds_str(geometry),
        polygon=get_geometry_str(geometry),
        rentMax=18500,
        numberOfRoomsMin=3,
    )

    base_url = settings.boligsiden_url

    # Search and send
    await search_and_send_listings(
        manager=manager,
        messenger=messenger,
        base_url=base_url,
        search_params=search_params,
    )


async def process_boligportal(manager: ListingsManager, messenger: DiscordMessenger):
    # Configure search parameters
    search_params = BoligportalSearchParams(
        max_monthly_rent=18500,
    )

    base_url = settings.boligportal_url

    # Search and send
    await search_and_send_listings(
        manager=manager,
        messenger=messenger,
        base_url=base_url,
        search_params=search_params,
    )


async def main():
    manager = ListingsManager(storage_path=Path("data/listings.parquet"))
    messenger = DiscordMessenger()

    # Boligsiden
    logger.info("Crawling Boligsiden.")
    # await process_boligsiden(manager=manager, messenger=messenger)

    # Boligportal
    logger.info("Crawling Boligportal.")
    await process_boligportal(manager=manager, messenger=messenger)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
