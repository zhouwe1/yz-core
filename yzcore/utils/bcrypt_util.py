#!/usr/bin/python3.6+
# -*- coding:utf-8 -*-
"""
@author: Allen
@date: 2020/9/30
@desc:
"""
from hmac import compare_digest
try:
    import bcrypt
except ImportError:
    bcrypt = None


class Bcrypt(object):

    _log_rounds = 12

    @classmethod
    def generate_password_hash(cls, password: str, rounds: int = None) -> str:
        """
        Generates a password hash using bcrypt. Specifying `rounds`
        sets the log_rounds parameter of `bcrypt.gensalt()` which determines
        the complexity of the salt. 12 is the default value.

        Example usage of :class:`generate_password_hash` might look something
        like this::

            pw_hash = bcrypt.generate_password_hash('secret', 10)

        :param password: The password to be hashed.
        :param rounds: The optional number of rounds.
        """
        if rounds is None:
            rounds = cls._log_rounds
        password = password.encode()
        return bcrypt.hashpw(password, bcrypt.gensalt(rounds)).decode()

    @staticmethod
    def check_password_hash(pw_hash: str, password: str) -> bool:
        """
        Tests a password hash against a candidate password. The candidate
        password is first hashed and then subsequently compared in constant
        time to the existing hash. This will either return `True` or `False`.

        Example usage of :class:`check_password_hash` would look something
        like this::

            pw_hash = bcrypt.generate_password_hash('secret', 10)
            bcrypt.check_password_hash(pw_hash, 'secret') # returns True

        :param pw_hash: The hash to be compared against.
        :param password: The password to compare.
        """
        pw_hash = pw_hash.encode()
        password = password.encode()
        return compare_digest(bcrypt.hashpw(password, pw_hash), pw_hash)
