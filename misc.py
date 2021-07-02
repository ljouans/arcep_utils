import os
import random
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
	

def first_file(rootpath: Union[str, Path]) -> Path:
	iterator = os.walk(rootpath)
	e = next(iterator)
	while e[2] == []:
		e = next(iterator)
	return Path(e[0]) / e[2][0]

def nth_file(rootpath: Union[str, Path], n: int) -> Path:
	iterator = os.walk(rootpath)
	e = next(iterator)
	while e[2] == []:
		e = next(iterator)
	
	return Path(e[0]) / e[2][n]




def random_first_file(rootpath: Union[str, Path]) -> Path:
	iterator = os.walk(rootpath)
	e = next(iterator)
	while e[2] == []:
		e = next(iterator)
	
	return Path(e[0]) / e[2][random.randint(0, len(e[2]))]

def iterate_files(rootpath: Union[str, Path]) -> Iterable[Path]:
	iterator = os.walk(rootpath)

	e = next(iterator, None)
	while e is not None:
		while e[2] == []:
			e = next(iterator)

		i = 0
		while i < len(e[2]):
			yield Path(e[0]) / e[2][i]
			i += 1
		e = next(iterator, None)
		

def insee_department_from_city(insee_city: str) -> str:
	dpt = insee_city[:2]
	if dpt in {'97', '98'}:
		dpt = insee_city[:3]
	return dpt