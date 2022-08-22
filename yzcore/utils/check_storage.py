from io import BytesIO
from yzcore.utils.crypto import get_random_string


def create_temp_file(text_length=16):
    file = BytesIO(get_random_string(text_length).encode())
    file.seek(0)
    return file
