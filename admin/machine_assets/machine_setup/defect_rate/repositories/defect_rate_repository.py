from typing import Optional, List, Dict, Any
from fastapi import HTTPException
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KPIDefectRateRepository:
    BOOKINGS_API_URL = "http://127.0.0.1:8000/bookings/bookings/"
    STATIONS_API_URL = "http://127.0.0.1:8000/stations/stations/"  

    def get_stations_map(self, token: Optional[str] = None) -> Dict[int, str]:
        """Retourne {station_id: station_name}"""
        if not token:
            return {}

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        try:
            response = requests.get(
                self.STATIONS_API_URL,
                headers=headers,
                timeout=30,
                verify=False
            )

            if response.status_code != 200:
                return {}

            data = response.json()

            # Gère list ou dict avec clé results/data/items
            stations = data if isinstance(data, list) else next(
                (data[k] for k in ("results", "data", "items", "stations") if isinstance(data.get(k), list)),
                []
            )

            return {
                int(s["id"]): s["name"]
                for s in stations
                if "id" in s and "name" in s
            }

        except Exception:
            return {}
    
    def get_bookings(
        self,
        station_id: Optional[int] = None,
        station_name: Optional[str] = None,  # ✅ ajouté
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
        if station_name is not None:  # ✅ ajouté
            params["station_name"] = station_name

        try:
            response = requests.get(
                self.BOOKINGS_API_URL,
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
            raise HTTPException(
                status_code=500,
                detail="Bookings API did not return valid JSON"
            )

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("results", "data", "items", "bookings"):  # ✅ ajout de "bookings"
                if isinstance(data.get(key), list):
                    return data[key]

        raise HTTPException(
            status_code=500,
            detail=f"Unexpected bookings format: {type(data).__name__}"
        )