from typing import Optional, List, Dict, Any
from fastapi import HTTPException
import requests
import urllib3

# Seulement pour test local
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KPIDefectRateRepository:
    BOOKINGS_API_URL = "https://core_demo.momes-solutions.com/bookings/bookings/"

    def get_bookings(
        self,
        station_id: Optional[int] = None,
        token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if not token:
            raise HTTPException(status_code=401, detail="Missing access token")

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        params = {}
        if station_id is not None:
            params["station_id"] = station_id

        try:
            response = requests.get(
                self.BOOKINGS_API_URL,
                headers=headers,
                params=params,
                timeout=30,
                verify=False  # uniquement pour test local
            )
        except requests.RequestException as e:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to call bookings API: {str(e)}"
            )

        if response.status_code == 401:
            raise HTTPException(status_code=401, detail="Unauthorized by bookings API")

        if response.status_code == 403:
            raise HTTPException(status_code=403, detail="Forbidden by bookings API")

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Bookings API returned {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
        except Exception:
            raise HTTPException(
                status_code=500,
                detail="Bookings API did not return valid JSON"
            )

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            if isinstance(data.get("results"), list):
                return data["results"]
            if isinstance(data.get("data"), list):
                return data["data"]
            if isinstance(data.get("items"), list):
                return data["items"]

        raise HTTPException(
            status_code=500,
            detail=f"Unexpected bookings format: {type(data).__name__}"
        )