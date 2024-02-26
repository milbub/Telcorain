class ProcessingException(Exception):
    """Exception raised for errors in the input data processing.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Error occurred during input data (pre)processing"):
        self.message = message
        super().__init__(self.message)
