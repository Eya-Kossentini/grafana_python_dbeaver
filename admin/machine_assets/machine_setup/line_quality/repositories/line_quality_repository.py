from typing import Optional, List, Dict, Any
from fastapi import HTTPException
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KPILineProductionQualityRepository:
    def __init__(self, bookings_url: str, lines_by_station_base_url: str) -> None:
        self.bookings_url = bookings_url
        self.lines_by_station_base_url = lines_by_station_base_url

    def get_bookings(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        station_id: Optional[int] = None,
        token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if not token:
            raise HTTPException(status_code=401, detail="Missing access token")

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        params: Dict[str, Any] = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if station_id is not None:
            params["station_id"] = station_id

        try:
            response = requests.get(
                self.bookings_url,
                headers=headers,
                params=params,
                timeout=30,
                verify=False
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
            raise HTTPException(status_code=500, detail="Bookings API did not return valid JSON")

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

    def get_lines_by_station(
        self,
        station_id: int,
        token: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        if not token:
            raise HTTPException(status_code=401, detail="Missing access token")

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        url = f"{self.lines_by_station_base_url}/{station_id}"

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=30,
                verify=False
            )
        except requests.RequestException as e:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to call lines by station API: {str(e)}"
            )

        if response.status_code == 401:
            raise HTTPException(status_code=401, detail="Unauthorized by lines API")
        if response.status_code == 403:
            raise HTTPException(status_code=403, detail="Forbidden by lines API")
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Lines API returned {response.status_code}: {response.text}"
            )

        try:
            data = response.json()
        except Exception:
            raise HTTPException(status_code=500, detail="Lines API did not return valid JSON")

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
            detail=f"Unexpected lines format: {type(data).__name__}"
        )