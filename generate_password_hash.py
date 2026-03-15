#!/usr/bin/env python3
"""
Script to generate password hash for authentication.
Run this to create a hashed password for your .env file.

Usage:
    python generate_password_hash.py
"""
import hashlib
import getpass

def hash_password(password: str) -> str:
    """Hash a password using SHA-256."""
    return hashlib.sha256(password.encode()).hexdigest()

def main():
    print("=" * 60)
    print("Password Hash Generator")
    print("=" * 60)
    print("\nThis will generate a secure hash for your password.")
    print("Add the output to your .env file as APP_PASSWORD_HASH\n")
    
    # Get password input
    while True:
        password = getpass.getpass("Enter your password: ")
        confirm = getpass.getpass("Confirm your password: ")
        
        if password == confirm:
            if len(password) < 8:
                print("❌ Password must be at least 8 characters long.\n")
                continue
            break
        else:
            print("❌ Passwords don't match. Please try again.\n")
    
    # Generate hash
    password_hash = hash_password(password)
    
    print("\n" + "=" * 60)
    print("✅ Password hash generated successfully!")
    print("=" * 60)
    print("\nAdd these lines to your .env file:")
    print("-" * 60)
    print(f"APP_USERNAME=admin")
    print(f"APP_PASSWORD_HASH={password_hash}")
    print(f"SESSION_TIMEOUT=480")
    print("-" * 60)
    print("\nNOTE: Keep this hash secure and never commit it to version control!")
    print("=" * 60)

if __name__ == "__main__":
    main()