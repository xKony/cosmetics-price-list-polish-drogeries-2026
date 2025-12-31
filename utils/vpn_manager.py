import time
import subprocess
import platform
from utils.logger import get_logger
from nordvpn_switcher_pro import VpnSwitcher
from nordvpn_switcher_pro.exceptions import NordVpnConnectionError


class VpnManager:
    """
    A utility class to manage NordVPN rotations with robust error handling
    and retry mechanisms.
    """

    def __init__(
        self,
        max_retries: int = 3,
        kill_wait_time: int = 5,
        reconnect_wait_time: int = 15,
    ):
        """
        Initialize the VpnManager.

        Args:
            max_retries (int): Number of times to retry after a critical error.
            kill_wait_time (int): Seconds to wait after killing the VPN process.
            reconnect_wait_time (int): Seconds to wait before attempting to rotate after a restart.
            location (str, optional): Specific location/country to rotate to (e.g., 'United States').
        """
        self.max_retries = max_retries
        self.kill_wait_time = kill_wait_time
        self.reconnect_wait_time = reconnect_wait_time
        self._switcher = VpnSwitcher()
        self.log = get_logger(__name__)

    def rotate_ip(self) -> bool:
        """
        Attempts to rotate the VPN IP. If a critical error occurs, it attempts
        to restart the VPN application and retry up to `max_retries` times.

        Returns:
            bool: True if rotation was successful, False otherwise.
        """
        # Initial session start attempt
        try:
            self._switcher.start_session()
        except Exception as e:
            self.log.warning(
                f"Initial session start flagged an issue: {e}. Proceeding to rotation logic."
            )

        attempt = 0
        while attempt <= self.max_retries:
            try:
                self.log.info(
                    f"Attempting to rotate VPN (Attempt {attempt + 1}/{self.max_retries + 1})..."
                )

                self._switcher.rotate()

                self.log.info("Rotation complete. Connection secured.")
                return True

            except (NordVpnConnectionError, Exception) as e:
                self.log.error(f"Rotation failed: {e}")

                if attempt < self.max_retries:
                    self.log.info("Initiating recovery sequence...")
                    self._handle_critical_error()
                    attempt += 1
                else:
                    self.log.critical("Max retries reached. Unable to rotate VPN.")
                    return False

        return False

    def _handle_critical_error(self) -> None:
        """
        Internal method to handle critical errors by killing the VPN process
        and waiting for the system to stabilize.
        """
        self._kill_vpn_process()

        self.log.info(f"Waiting {self.kill_wait_time}s for process termination...")
        time.sleep(self.kill_wait_time)

        try:
            self.log.info("Attempting to restart VPN session...")
            self._switcher.start_session()

            self.log.info(
                f"Session started. Waiting {self.reconnect_wait_time}s for network stability..."
            )
            time.sleep(self.reconnect_wait_time)
        except Exception as e:
            self.log.error(f"Error during recovery session start: {e}")

    def _kill_vpn_process(self) -> None:
        """
        Kills the NordVPN process. Detects OS to ensure safety.
        """
        system = platform.system()
        self.log.info(f"Killing NordVPN process on {system}...")

        if system == "Windows":
            # /F = Force, /IM = Image Name, /T = Tree (child processes)
            subprocess.run(
                "taskkill /F /IM nordvpn.exe /T",
                shell=True,
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
            # Run twice just to be sure, as per original logic
            subprocess.run(
                "taskkill /F /IM nordvpn.exe /T",
                shell=True,
                stderr=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
            )
        elif system == "Linux":
            subprocess.run("pkill -f nordvpn", shell=True)
        elif system == "Darwin":  # macOS
            subprocess.run("pkill -f NordVPN", shell=True)
        else:
            self.log.warning("Unknown Operating System. Skipping process kill.")
