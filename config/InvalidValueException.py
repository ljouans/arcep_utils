"""
Exception pour les paramètres invalides lus en configuration.
Peu utilisée.
"""


class InvalidValueError(ValueError):
    """
    Erreur: paramètre invalide lu dans la configuration
    """

    def __init__(self,
                 message: str,
                 section: str,
                 key: str,
                 given_value: str,
                 ):
        super().__init__(message)

        self.section = section
        self.key = key
        self.given_value = given_value
