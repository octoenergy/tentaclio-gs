"""Client exceptions."""
from tentaclio.clients.exceptions import ClientError


class GSError(ClientError):
    """Exception encountered over a GS client connection."""
