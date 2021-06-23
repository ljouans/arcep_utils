import os
from pathlib import Path
from typing import Union


def _get_tool_path() -> Path:
    return Path(os.path.realpath(__file__)).parent

def data_path() -> Path:
    path = (_get_tool_path() /  '../data').resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path
	
def prod_path() -> Path:
    path = _get_tool_path().parent / 'data/prod/'
    path.mkdir(parents=True, exist_ok=True)
    return path

def tmp_path() -> Path:
    path = (_get_tool_path() / '../data/tmp').resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path

def specific_datapath(stem: Union[str, Path]):
    path = data_path() / stem
    path.mkdir(parents=True, exist_ok=True)
    return path