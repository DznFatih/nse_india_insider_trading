

class PossibleDBInjectionValueFound(Exception):

    def __init__(self, message: str):
        """
        Custom error to raise in case of DB injection attack
        :param message:
        """

        # Call the base class constructor with the provided message
        super().__init__(message)
