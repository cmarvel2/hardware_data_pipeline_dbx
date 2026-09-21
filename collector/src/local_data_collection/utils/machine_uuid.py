import logging
import uuid

logger = logging.getLogger(__name__)


def get_windows_uuid() -> str:
    import winreg

    key_handle = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography", 0, winreg.KEY_READ)
    uuid_tuple = winreg.QueryValueEx(key_handle, "MachineGuid")
    winreg.CloseKey(key_handle)

    collected_uuid, _ = uuid_tuple
    validuuid = uuid.UUID(str(collected_uuid))
    logger.info("Valid windows UUID collected")

    return str(validuuid)
