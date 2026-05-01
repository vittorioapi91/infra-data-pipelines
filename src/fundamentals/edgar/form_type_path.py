"""
Filesystem-safe SEC form type strings for storage paths (avoid ``/`` in directory names).
"""


def form_type_filesystem_slug(form_type: str) -> str:
    """
    Single path segment for a SEC form type: ``/`` and ``\\`` become ``_``
    (e.g. ``10-K/A`` → ``10-K_A``).
    """
    if not form_type:
        return form_type
    return str(form_type).replace("\\", "_").replace("/", "_")
