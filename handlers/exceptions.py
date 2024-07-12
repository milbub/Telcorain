class ConfigOptionNotFound(Exception):
    def __init__(self, section: str, option: str):
        self.section = section
        self.option = option
        super().__init__(
            f"ERROR: Missing option in configuration file. Check the config! Section: {section}, Option: {option}"
        )
