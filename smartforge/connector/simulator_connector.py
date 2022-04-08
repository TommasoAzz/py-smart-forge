import logging
import requests
from json import loads as json_loads


"""
Logger
"""
logger = logging.getLogger("SimulatorConnector")
logger.setLevel(level=logging.INFO)


class SimulatorConnector:
    def __init__(self, host: str) -> None:
        logger.info(f"Creating a new SimulatorConnector connecting to {host}.")
        self._host = host

    def default_parameters(self) -> dict:
        response = requests.post(f"{self.host}/default_parameters")
        if response.ok:
            return json_loads(response.text)
        else:
            logger.error("The request to the simulator did not succeed.")
            return {}