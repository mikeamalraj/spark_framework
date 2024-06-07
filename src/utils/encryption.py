from cryptography.fernet import Fernet


def generate_secret_key() -> bytes:
    return Fernet.generate_key()


def encrypt_message(secret: str, msg: str):
    cipher_suite = Fernet(secret.encode())
    return cipher_suite.encrypt(msg.encode())


def decrypt_message(secret: str, encrypted_msg: str):
    if secret != "" and encrypted_msg != "":
        cipher_suite = Fernet(secret.encode())
        return cipher_suite.decrypt(encrypted_msg.encode()).decode()
    else:
        raise Exception("Invalid secret message")


if __name__ == "__main__":
   pass