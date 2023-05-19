import configparser
from os.path import exists


class ConfigManager:
    def __init__(self):
        self.config_path = './config.ini'

        self.configs = configparser.ConfigParser()
        self.sections = []

        if exists(self.config_path):
            self.configs.read(self.config_path, encoding='utf-8')
            self.sections = self.configs.sections()
        else:
            # TODO: do some clever exit from the app
            raise FileNotFoundError("ERROR: Cannot start! Missing configuration file!")

    def read_option(self, section, option):
        if not self.configs.has_option(section, option):
            # TODO: do some clever exit from the app
            raise ModuleNotFoundError("ERROR: Missing option in configuration file. Check the config!")

        return self.configs[section][option]

    def load_sql_config(self) -> {}:
        sql_configs = {
            'address': self.read_option('mariadb', 'address'),
            'port': self.read_option('mariadb', 'port'),
            'user': self.read_option('mariadb', 'user'),
            'pass': self.read_option('mariadb', 'pass'),
            'timeout': self.read_option('mariadb', 'timeout'),
            'db_metadata': self.read_option('mariadb', 'db_metadata'),
            'db_output': self.read_option('mariadb', 'db_output')
        }

        return sql_configs
