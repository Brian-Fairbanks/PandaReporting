class ArcGISLoginError(Exception):
    """Exception raised for login errors.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Invalid username or password."):
        self.message = message
        super().__init__(self.message)
