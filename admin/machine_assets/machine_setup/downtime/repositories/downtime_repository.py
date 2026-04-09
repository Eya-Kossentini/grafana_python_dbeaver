import requests
import urllib3
from fastapi import HTTPException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class KPIDowntimeRepository:
    def __init__(
        self,
        machine_conditions_url: str,
        machine_condition_data_url: str,
    ) -> None:
        self.machine_conditions_url = machine_conditions_url
        self.machine_condition_data_url = machine_condition_data_url

    def get_machine_conditions(self, token: str | None = None):
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        response = requests.get(
            self.machine_conditions_url,
            headers=headers,
            timeout=30,
            verify=False,
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Error fetching machine conditions: {response.text}"
            )

        data = response.json()
        if not isinstance(data, list):
            raise HTTPException(
                status_code=500,
                detail="Invalid machine conditions API response format"
            )

        return data

    def get_machine_condition_data(self, token: str | None = None):
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        response = requests.get(
            self.machine_condition_data_url,
            headers=headers,
            timeout=30,
            verify=False,
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Error fetching machine condition data: {response.text}"
            )

        data = response.json()
        if not isinstance(data, list):
            raise HTTPException(
                status_code=500,
                detail="Invalid machine condition data API response format"
            )

        return data