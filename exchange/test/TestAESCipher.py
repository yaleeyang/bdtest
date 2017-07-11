#!/Users/sunny/anaconda/bin/python
import unittest

from exchange.AESCipher import AESCipher

KEY = 'mABKue3DGqxuNQh6'
IV = 'cdcd8132-ae1a-40'

class TestAESCipher(unittest.TestCase):

    def test_encrypt(self):
        ciper = AESCipher(KEY, IV)
        encrypted_text = ciper.encrypt('Test String')
        self.assertEqual(encrypted_text, b"W2pMWKw3/a1OQwRR9NCE1w==")

    def test_decrypt(self):
        ciper = AESCipher(KEY, IV)
        decrypted_text = ciper.decrypt(b"W2pMWKw3/a1OQwRR9NCE1w==")
        self.assertEqual(decrypted_text, b"Test String")


if __name__ == '__main__':
    unittest.main()

