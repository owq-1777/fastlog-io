import string

from nanoid import generate

__all__ = ['generate_id']


def generate_id(size: int = 6, *, digits: bool = False) -> str:
    '''Return a random NanoID (digits‑only when *digits* is True).'''
    alphabet = string.digits if digits else string.ascii_letters + string.digits
    return generate(alphabet, size=size)
