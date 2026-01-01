"""
Configuration Settings
This file contains the global settings for the scraping pipeline, including
network behavior, file I/O, and target URLs.
"""

# =============================================================================
# GENERAL SETTINGS
# =============================================================================
# Enable or disable persisting logs to disk
SAVE_LOGS = True

# Logging verbosity: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
LOG_LEVEL = "DEBUG"

# Whether to run browser automation in headless mode (if applicable)
HEADLESS_BROWSER = False


# =============================================================================
# SCRAPING BEHAVIOR
# =============================================================================
# The base category URL to target for the scraping session
NOTINO_URL = "https://www.notino.pl/kosmetyka/kosmetyki-do-twarzy/"

# Limit the number of products to scrape to prevent infinite loops or excessive data
MAX_PRODUCTS = 2000

# Polite Scraping: Random delay interval (in seconds) between requests
SCRAPE_INTERVAL_MIN = 1.0
SCRAPE_INTERVAL_MAX = 1.5


# =============================================================================
# NETWORK & VPN CONFIGURATION
# =============================================================================
# Toggle NordVPN integration. Set to False for local debugging without VPN.
USE_NORDVPN = True

# VPN Rotation Strategy
# Rotates the IP address after a random number of requests between MIN and MAX.
VPN_ROTATE_MIN = 15
VPN_ROTATE_MAX = 35

