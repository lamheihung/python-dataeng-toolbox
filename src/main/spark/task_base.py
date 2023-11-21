from typing import Union
import json
from abc import ABC


class TaskBase(ABC):
    """TaskBase is the foundamental of application.

    Args:
        ABC (class): ABC in abc module
    """
    def __init__(self,
                 config: Union[str, dict[str, Union[str, dict[str, str]]]]):
        """Constructs all the necessary attributes for the TaskBase.

        Args:
            config (Union[str, dict[str, Union[str, dict[str, str]]]]): The dictionary of the config or the json path of the configuration.
        """
        super().__init__()
        self.config = config
    # ------------------------------
    # property
    @property
    def config(self) -> dict[str, Union[str, dict[str, str]]]:
        return self._config
    
    @config.setter
    def config(self, 
               value: Union[str, dict[str, Union[str, dict[str, str]]]]):
        try:
            if isinstance(value, dict):
                self._config = value
            elif isinstance(value, str):
                self._config = self.load_config(value)
            else:
                raise ValueError(f"Unknown configuration: {value}")
            print("Configuration is validated.")
        except:
            raise ValueError("Input configuration must be either dict type or file path in json format. Please check the config param value.")
    # ------------------------------
    def load_config(self, 
                    filepath: str) -> dict[str, Union[str, dict[str, str]]]:
        """Read the configuration in json format.

        Args:
            filepath (str): The file path of the json file.

        Returns:
            dict[str, Union[str, dict[str, str]]]: The dictionary of the configuration
        """
        with open(filepath, 'rb') as outfile:
            data = outfile.read()
            config = json.loads(data)
            return config