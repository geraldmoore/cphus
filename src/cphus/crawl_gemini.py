from typing import Literal
from urllib.parse import urlencode

import httpx
from pydantic import BaseModel, Field
from pydantic_ai import Agent
from pydantic_ai.models.google import GoogleModel
from pydantic_ai.providers.google import GoogleProvider

from src.cphus.core.config.logging import get_logger
from src.cphus.core.config.settings import get_settings

settings = get_settings()

logger = get_logger(__name__)


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


provider = GoogleProvider(api_key=settings.gemini_api_key.get_secret_value())
model = GoogleModel("gemini-2.5-pro", provider=provider)
agent = Agent(
    model=model,
    output_type=Output,
    instructions=(
        "You are an expert at extracting structured data from HTML. "
        "Analyze the provided Danish HTML content and extract property listings in English"
        "according to the specified schema. Be thorough and accurate. "
        "If a field is not present in the HTML, omit it or set it to None. "
        "Extract all listings you can find on the page."
    ),
)


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

    @staticmethod
    async def fetch_html(url: str, timeout: float = 30.0) -> str:
        """
        Fetch HTML content from a URL.

        Args:
            url: URL to fetch
            timeout: Request timeout in seconds

        Returns:
            HTML content as string

        Raises:
            httpx.HTTPError: If request fails
        """
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "da,en-US;q=0.9,en;q=0.8",
            }
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return response.text

    async def scrape_listings(
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

            html = await self.fetch_html(url)

            result = await agent.run(html)

            page_listings = result.output.listings
            if not page_listings:
                next_page = False
                break

            all_listings.extend(page_listings)
            page += 1

        return all_listings

    async def scrape_all_listings(self):
        pages = 999
        return await self.scrape_listings(pages=pages)
