import requests
from json import loads as json_loads
from ..utils import get_logger

"""
Logger
"""
logger = get_logger("OPCUAConnector")


class SimulatorConnector:
    def __init__(self, host: str) -> None:
        logger.info(f"Creating a new SimulatorConnector connecting to {host}.")
        self._host = host

    def default_parameters(self) -> dict:
        response = requests.post(f"{self._host}/default_parameters")
        if response.ok:
            return json_loads(response.text)
        else:
            logger.error("The request to the simulator did not succeed.")
            return {}