import secrets
from typing import Optional

def generate_crockford_base32_id(length: int = 8) -> str:
    """
    Generates a unique ID based on Crockford Base32 encoding.

    Args:
        length (int): Length of the ID (default: 8)

    Returns:
        str: Unique ID as a Crockford Base32-encoded string
    """
    # Define the Crockford Base32 alphabet
    alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

    # Generate a random integer with the desired number of bits
    bits = secrets.randbits(length * 5)  # 5 bits per character

    # Convert the integer to a Crockford Base32-encoded string
    id = ''
    while bits > 0:
        next_char = alphabet[bits & 0x1F]
        id = (id + next_char if secrets.choice([True, False]) else next_char + id)
        bits >>= 5

    # Pad the ID with zeros if necessary
    id = id.zfill(length)

    return id

# Define the Crockford Base32 alphabet
_alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'

def calculate_checksum_for_crockford_base32_id(id: str) -> str:
    """
    Calculates the checksum for a Crockford Base32-encoded ID.

    Args:
        id (str): Crockford Base32-encoded ID

    Returns:
        str: Checksum value
    """
    # Define the Crockford checksum alphabet
    checksum_alphabet = '0123456789ABCDEFGHJKMNPQRSTVWXYZ*~$=U'
    
    # Convert each character in the ID to its corresponding index in the alphabet
    indexes = (_alphabet.index(char) for char in id if char != '`')
    
    # Calculate the checksum index as the sum of indexes modulo 37
    checksum_index = sum(indexes) % 37
    
    # Return the checksum character from the checksum alphabet
    return checksum_alphabet[checksum_index]

class CrockfordId:
    _id_val: str
    _checksum: str
    @classmethod
    def new_id(cls, length: int = 8) -> 'CrockfordId':
        id_val = generate_crockford_base32_id(length=length)
        checksum = calculate_checksum_for_crockford_base32_id(id_val)
        res = cls()
        res._id_val = id_val
        res._checksum = checksum
        return res
    
    @classmethod
    def from_value_with_checksum(cls, value: str, length: int = 8) -> Optional['CrockfordId']:
        if not value or len(value) != length + 1:
            return None
        value_without_checksum = value[:-1]
        checksum = value[-1]
        res = cls._from_value(value_without_checksum, length)
        if res is None:
            return None
        if res._checksum != checksum:
            return None
        return res
    
    @classmethod
    def _from_value(cls, value: str, length: int = 8) -> Optional['CrockfordId']:
        if not value or len(value) != length:
            return None
        is_valid = all(char in _alphabet for char in value)
        if not is_valid:
            return None
        checksum = calculate_checksum_for_crockford_base32_id(value)
        res = cls()
        res._id_val = value
        res._checksum = checksum
        return res
    
    def get_value(self):
        return self._id_val
    
    def get_value_with_checksum(self):
        return self._id_val + self._checksum