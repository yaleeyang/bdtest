import binascii
import base64
from Crypto.Cipher import AES

MODE = AES.MODE_CBC
BLOCK_SIZE = 16
SEGMENT_SIZE = 128

pdd = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpdd = lambda s : s[0:-(s[-1])]
class AESCipher:

    def __init__( self, key, iv ):
        self.key = key
        self.iv = iv

    def encrypt( self, raw ):
        aes = AES.new(self.key, MODE, self.iv, segment_size=SEGMENT_SIZE)
        plaintext = self._pad_string(raw)
        encrypted_text = aes.encrypt(plaintext)
        return base64.b64encode(encrypted_text)

    def decrypt( self, enc ):
        aes = AES.new(self.key, MODE, self.iv, segment_size=SEGMENT_SIZE)
        encrypted_text_bytes = base64.b64decode(enc)
        decrypted_text = aes.decrypt(encrypted_text_bytes)
        decrypted_text = self._unpad_string(decrypted_text)
        return decrypted_text

    def _pad_string(self, value):
        return pdd(value)

    def _unpad_string(self, value):
        return unpdd(value)
