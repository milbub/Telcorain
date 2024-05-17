class ProcessingException(Exception):
    """
    Exception raised for errors in the input data processing.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred during input data (pre)processing"):
        self.message = message
        super().__init__(self.message)


class RaincalcException(Exception):
    """
    Exception raised for errors during rainfall calculation process.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred during rainfall calculation processing"):
        self.message = message
        super().__init__(self.message)


class RainfieldsGenException(Exception):
    """
    Exception raised for errors during rainfall fields generation.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred during rainfall fields generation"):
        self.message = message
        super().__init__(self.message)