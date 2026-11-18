"""Exception hierarchy for the caller_prober package."""


class ProberError(Exception):
    """Base class for all caller_prober failures."""


class ProbeConfigError(ProberError):
    """Raised when probe_config.yml is missing, malformed, or inconsistent."""


class AriProbeError(ProberError):
    """Raised when an ARI probe operation fails at the transport level."""
