import secrets

def _generate_crockford_base32_id(length: int = 8) -> str:
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

def _calculate_checksum_for_crockford_base32_id(id: str) -> str:
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

class CrockfordId(str):
    def __new__(cls, value: str):
        instance = super().__new__(cls, value)
        return instance
    
    @staticmethod
    def new_id(length: int = 8):
        id_val = _generate_crockford_base32_id(length=length)
        return CrockfordId(id_val)
    
    @staticmethod
    def from_value_with_checksum(value: str, length: int = 8):
        if not value or len(value) != length + 1:
            return None
        value_without_checksum = value[:-1]
        checksum = value[-1]
        res = CrockfordId.from_value(value_without_checksum, length)
        if res is None:
            return None
        calculated_checksum = _calculate_checksum_for_crockford_base32_id(res)
        if calculated_checksum != checksum:
            return None
        return res
    
    @staticmethod
    def from_value(value: str, length: int):
        if not value or len(value) != length:
            return None
        is_valid = all(char in _alphabet for char in value)
        if not is_valid:
            return None
        return CrockfordId(value)
    
    def get_value_with_checksum(self):
        calculated_checksum = _calculate_checksum_for_crockford_base32_id(self)
        return self + calculated_checksum