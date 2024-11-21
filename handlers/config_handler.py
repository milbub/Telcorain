"""Module for reading configuration file."""
import configparser
from os.path import exists

from handlers.exceptions import ConfigOptionNotFound


class ConfigHandler:
    """Class for handling and reading configuration file."""
    def __init__(self):
        self.config_path = "./config.ini"

        self.configs = configparser.ConfigParser()
        self.sections = []

        if exists(self.config_path):
            self.configs.read(self.config_path, encoding="utf-8")
            self.sections = self.configs.sections()
        else:
            raise FileNotFoundError("Missing configuration file! Check the config.ini file in root directory.")

    def read_option(self, section: str, option: str) -> str:
        """
        Reads a specific option from a specific section in the configuration file.

        :param section: The section in the configuration file.
        :param option: The option in the section.
        :return: The value of the option.

        :raises ConfigOptionNotFound: If the option is not found in the configuration file.
        """
        if not self.configs.has_option(section, option):
            raise ConfigOptionNotFound(section, option)
        return self.configs[section][option]

    def load_sql_config(self) -> dict[str, str]:
        """
        Loads the MariaDB configuration from the configuration file.
        :return: A dictionary containing the MariaDB configuration.
        """
        sql_configs = {
            "address": self.read_option("mariadb", "address"),
            "port": self.read_option("mariadb", "port"),
            "user": self.read_option("mariadb", "user"),
            "pass": self.read_option("mariadb", "pass"),
            "timeout": self.read_option("mariadb", "timeout"),
            "db_metadata": self.read_option("mariadb", "db_metadata"),
            "db_output": self.read_option("mariadb", "db_output")
        }

        return sql_configs
