from typing import List


class InvalidValueError(ValueError):
    """Erreur: paramètre invalide lu dans la configuration
    """

    def __init__(
        self,
        message: str,
        section: str,
        key: str,
        given_value: str,
        expected_values: List[str] = None,
    ):
        super().__init__(message)

        expected_values = []
        self.section = section
        self.key = key
        self.given_value = given_value
        self.expected_values = expected_values
