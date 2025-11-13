#!/usr/bin/env python3
"""ASCII Art and branding for CrocQueuectl"""

CROC_LOGO = r"""
╔════════════════════════════════════════════╗
║                                            ║
║   🐊 CrocQueuectl - Job Queue System       ║
║                                            ║
╚════════════════════════════════════════════╝
"""

def print_logo():
    """Print the main CrocQueuectl logo"""
    print("\033[32m" + CROC_LOGO + "\033[0m")  # Green color

if __name__ == "__main__":
    # Demo the clean ASCII art
    print_logo()
