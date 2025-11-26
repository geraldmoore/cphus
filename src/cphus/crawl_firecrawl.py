from typing import Literal
from urllib.parse import urlencode

from firecrawl import Firecrawl
from pydantic import BaseModel, Field

from src.cphus.core.config.logging import get_logger
from src.cphus.core.config.settings import get_settings

settings = get_settings()

logger = get_logger(__name__)

firecrawl = Firecrawl(api_key=settings.firecrawl_api_key.get_secret_value())


class Listing(BaseModel):
    monthly_rent: int
    deposit: int
    square_metres: int
    address: str
    rental_period: str
    number_rooms: int
    listing_url: str


class Output(BaseModel):
    listings: list[Listing] | None


class BoligsidenSearchParams(BaseModel):
    sortBy: Literal["createdAt", "averageRent", "rentalPeriod", "deposit"] = Field(
        default="createdAt"
    )
    mapBounds: str | None = Field(default=None)
    polygon: str | None = Field(default=None)
    numberOfRoomsMin: int | None = Field(default=None)
    numberOfRoomsMax: int | None = Field(default=None)
    rentMin: int | None = Field(default=None)
    rentMax: int | None = Field(default=None)
    depositMin: int | None = Field(default=None)
    depositMax: int | None = Field(default=None)
    floorMin: int | None = Field(default=None)
    floorMax: int | None = Field(default=None)
    areaMin: int | None = Field(default=None)
    areaMax: int | None = Field(default=None)
    page: int | None = Field(default=None)

    def set_pagination(self, page: int) -> None:
        self.page = page


class BoligportalSearchParams(BaseModel):
    order: Literal["RENT_ASC", "RENT_DESC", "SIZE_M2_ASC", "SIZE_M2_DESC"] | None = Field(
        default=None
    )
    max_monthly_rent: int | None = Field(default=None)
    min_size_m2: int | None = Field(default=None)
    offset: int | None = Field(default=None)

    def set_pagination(self, page: int) -> None:
        self.offset = (page - 1) * 18


class CrawlListings:
    def __init__(
        self, base_url: str, search_params: BoligsidenSearchParams | BoligportalSearchParams
    ) -> None:
        self.base_url = base_url
        self.search_params = search_params

    def scrape_listings(
        self,
        pages: int = 1,
    ):
        all_listings = []
        page = 1
        next_page = True
        while next_page and (page <= pages):
            logger.info(f"Collecting page {page} of {pages}.")

            # Set pagination
            self.search_params.set_pagination(page)

            params_dict = self.search_params.model_dump(exclude_none=True)
            url = f"{self.base_url}?{urlencode(params_dict)}"

            scrape_status = firecrawl.scrape(
                url,
                formats=[
                    "markdown",
                    {"type": "json", "schema": Output.model_json_schema()},
                ],
            )

            page_listings = scrape_status.json.get("listings")
            if not page_listings:
                next_page = False
                break

            all_listings.extend(page_listings)
            page += 1

        return all_listings

    def scrape_all_listings(self):
        pages = 999
        return self.scrape_listings(pages=pages)
