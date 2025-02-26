"""
Load project configurations from .env files and provide easy access to paths
and credentials used in the project. This module is typically imported elsewhere.

If this file (`settings.py`) is run on its own, it will create the appropriate
directories (e.g., data and output directories).

The rationale behind using `python-decouple`:
    - Helps ensure the project has "only one configuration module"
    - Enables different .env files for different environments (e.g., development, production)
    - Keeps credentials and sensitive settings out of version control

Usage:
    from settings import config
    DATA_DIR = config("DATA_DIR")

Example:
    # In a .env file:
    DATA_DIR=_data
    # Then in your code:
    >>> from settings import config
    >>> print(config("DATA_DIR"))
    /path/to/project/_data

Note:
    If you need to switch environments, copy the relevant variables
    into `.env` and reload the project. 
"""

import sys
from pathlib import Path
from platform import system

from pandas import to_datetime
from decouple import config as _config

def get_os():
    """
    Detect the operating system in use. Returns 'windows' if Windows,
    'nix' if macOS or Linux, and 'unknown' otherwise.
    """
    os_name = system()
    if os_name == "Windows":
        return "windows"
    elif os_name == "Darwin":
        return "nix"
    elif os_name == "Linux":
        return "nix"
    else:
        return "unknown"


def if_relative_make_abs(path):
    """
    If a relative path is provided, convert it into an absolute path
    relative to BASE_DIR. If the path is already absolute, it is
    simply resolved to its canonical form.

    Parameters
    ----------
    path : str or Path
        The path to convert.

    Returns
    -------
    Path
        The absolute path, resolved from BASE_DIR if initially relative.

    Examples
    --------
    >>> if_relative_make_abs(Path('_data'))
    WindowsPath('C:/Users/jdoe/MyRepo/_data')

    >>> if_relative_make_abs(Path('C:/absolute/path/_output'))
    WindowsPath('C:/absolute/path/_output')
    """
    path = Path(path)
    if path.is_absolute():
        return path.resolve()
    return (d["BASE_DIR"] / path).resolve()


# Dictionary that holds all config values
d = {}

# Detect OS
d["OS_TYPE"] = get_os()

# Absolute path to the root directory of this project
d["BASE_DIR"] = Path(__file__).absolute().parent.parent

# ------------------------------------------------------------------------------
# .env Variables
# ------------------------------------------------------------------------------
# Dates
d["START_DATE"] = _config("START_DATE", default="1913-01-01", cast=to_datetime)
d["END_DATE"]   = _config("END_DATE",   default="2024-01-01", cast=to_datetime)

# Misc
d["PIPELINE_DEV_MODE"] = _config("PIPELINE_DEV_MODE", default=True, cast=bool)
d["PIPELINE_THEME"]    = _config("PIPELINE_THEME",    default="pipeline")

# Paths
d["DATA_DIR"]        = if_relative_make_abs(_config("DATA_DIR",        default=Path('_data'),       cast=Path))
d["MANUAL_DATA_DIR"] = if_relative_make_abs(_config("MANUAL_DATA_DIR", default=Path('data_manual'), cast=Path))
d["OUTPUT_DIR"]      = if_relative_make_abs(_config("OUTPUT_DIR",      default=Path('_output'),     cast=Path))
d["PUBLISH_DIR"]     = if_relative_make_abs(_config("PUBLISH_DIR",     default=Path('_output/publish'), cast=Path))

# Stata executable name
if d["OS_TYPE"] == "windows":
    d["STATA_EXE"] = _config("STATA_EXE", default="StataMP-64.exe")
elif d["OS_TYPE"] == "nix":
    d["STATA_EXE"] = _config("STATA_EXE", default="stata-mp")
else:
    raise ValueError("Unknown OS type detected.")


def config(key, default=None, cast=None):
    """
    Retrieve a configuration variable by name.

    This function checks:
      1) The local dictionary `d` first (i.e., variables we stored after reading `.env`)
      2) If not found, it falls back to `decouple.config` directly.

    Parameters
    ----------
    key : str
        The name of the config variable to retrieve.
    default : optional
        A default value if the key isn't found in `d` or .env.
    cast : optional
        A callable used to cast the retrieved value (e.g., int, float, bool, etc.).

    Returns
    -------
    Any
        The requested configuration value, possibly cast to a certain type.

    Raises
    ------
    ValueError
        If trying to assign a new default for an existing variable,
        or if attempting to cast an already-defined variable to a different type.
    """
    if key in d:
        var = d[key]
        if default is not None:
            # We already have a default in the dictionary, so a new default is redundant
            raise ValueError(f"Default for {key} already exists. Check your settings.py file.")

        if cast is not None:
            # We allow re-emphasizing the type, but not changing it
            test_cast = cast(var)
            if type(test_cast) is not type(var):
                raise ValueError(f"Type for {key} is already set. Check your settings.py file.")

    else:
        # If not in the local dict, use decouple normally
        var = _config(key, default=default, cast=cast)

    return var


def create_dirs():
    """
    Create the project's essential directories if they do not exist.
    Typically used when running this file as a script.
    """
    d["DATA_DIR"].mkdir(parents=True, exist_ok=True)
    d["OUTPUT_DIR"].mkdir(parents=True, exist_ok=True)
    (d["BASE_DIR"] / "_docs").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    create_dirs()
