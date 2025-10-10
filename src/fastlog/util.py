from fastnanoid import generate

__all__ = ['generate_id']


def generate_id(size: int = 18, *, digits: bool = False) -> str:
    """Return a random NanoID (digits‑only when *digits* is True)."""

    alphabet = '0123456789' if digits else 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return generate(alphabet, size=size)
