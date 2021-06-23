from pathlib import Path
from typing import Any, Iterable, Optional, Sized, Union

import tqdm

from . import pathtools as pth
import pickle

CuPath = Optional[Union[str, Path]]
def _clean_path(filepath: CuPath=None, basepath: CuPath = None, filename: CuPath= None) -> Path: # type: ignore
	if filepath is not None:
		return Path(filepath)
	elif basepath is not None and filename is not None:
		return Path(basepath) / filename
	else:
		print("Error. Please provide at least a filename or a filepath in {save} function.")
		exit(1)

def save( data: Any,filename: Optional[str] = None, filepath : Union[str, Path] = None, basepath: Optional[Union[str, Path]] = None):
	path = _clean_path(filepath, basepath, filename)
	with open(str(path), 'wb') as f:
		pickle.dump(data, f)

def load(filepath: CuPath = None, filename:CuPath = None, basepath: CuPath = None):
	path = _clean_path(filepath, basepath, filename)
	with open(str(path), 'rb') as f:
		return pickle.load(f)

def make_iterator(iterator: Union[Sized, Iterable], low_bound: int = 100, size: Optional[int] = None, desc: str = "", leave: bool = False) -> Any:
	if size is None:
		size = len(iterator) # type: ignore

	if size > low_bound:
		return tqdm.tqdm(iterator, desc=desc, total=size, leave=leave)
	else:
		return iterator
	