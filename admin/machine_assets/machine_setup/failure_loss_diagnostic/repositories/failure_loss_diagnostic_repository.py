import requests
import urllib3
from fastapi import HTTPException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KPIFailureLossDiagnosticRepository:
    def __init__(self, bookings_url: str) -> None:
        self.bookings_url = bookings_url

    def get_bookings_data(self, token: str | None = None):
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        response = requests.get(
            self.bookings_url,
            headers=headers,
            timeout=30,
            verify=False,
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Error fetching bookings data: {response.text}"
            )

        data = response.json()
        if not isinstance(data, list):
            raise HTTPException(
                status_code=500,
                detail="Invalid bookings API response format"
            )

        return data