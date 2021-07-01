## Tools & helpers
import logging
import hashlib
import os
from typing import Any, Callable, Dict, Optional, Sequence, Union
import fiona
import geopandas as pdg
from geopandas.geodataframe import GeoDataFrame  # type: ignore
import pandas as pd
import warnings; warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')
from configparser import ConfigParser
from sqlalchemy import create_engine
import sqlalchemy as sqa
from . import pathtools as pth
from pathlib import Path

class Tool:
	def __init__(self, tmpDir: Optional[Path] = None, userFileName: str='', secretPathFile: Optional[Union[Path, str]] = None):
		if tmpDir is None:
			tmpDir = pth.tmp_path()

		self._tmp = tmpDir
		self._userFileName = userFileName
		self._thisRandom = str(hash(userFileName))
		self._engine = self._create_engine(secretPathFile)
		self._connexion_string = ''
		
		
	@property
	def connexion_string(self):
		return self._connexion_string

	@property
	def tmp(self):
		return self._tmp / self._thisRandom
	
	
	def _create_dir(self, folderPath:Path):
		folderPath.mkdir(exist_ok=True)

	def _create_engine(self, secretPathFile: Optional[Union[str, Path]] = None):
		parser = ConfigParser()

		secretpath = pth._get_tool_path() / 'secret/db.cfg'
		secretpath = Path(secretPathFile) if secretPathFile is not None else secretpath

		if not secretpath.exists():
			raise FileNotFoundError('Could not find the secret folder. Please specify the connexion secrets')

		_ = parser.read(secretpath)

		engine = create_engine(parser.get('Collecte03_dev', 'conn_string'))
		self._connexion_string = parser.get('Collecte03_dev', 'conn_string')
		return engine

	def has_table(self, table_name: str, schema: str) -> bool:
		insp = sqa.inspect(self._engine)
		return insp.has_table(table_name, schema=schema)
		
	def _get_crs(self, table_name: str, geo_col: str, schema: str) -> str:
		df = pd.read_sql(f'SELECT ST_SRID({geo_col}) FROM {schema}.{table_name} where {geo_col} is not NULL LIMIT 1;', self._engine)
		return str(df['st_srid'].values[0])
		
		
	def fetch_query(self, query: str, storeIt: bool=True, geo_col: Optional[str]=None, geo_table_name: Optional[str] = None, geo_schema_name: str = 'test_loic', force_refetch: bool=False, params: Optional[Dict[str, Any]]=None) -> Union[pd.DataFrame, pdg.GeoDataFrame]:

		crs = None
		if geo_col is not None:
			if geo_table_name is None:
				logging.warn('I strongly suggest you specify the name of the table containing the geographic informations so that I can get the right CRS.')
			else:
				crs = 'EPSG:' + self._get_crs(geo_table_name, geo_col, schema=geo_schema_name)
				logging.debug(f'Found CRS = {crs}')
		   


		if geo_col is None:
			_loader = pd.read_feather # type: ignore
		else:
			_loader = pdg.read_feather # type: ignore
		_loader: Callable[[str, Optional[Sequence[str]], Optional[bool]], pd.DataFrame]

		self._create_dir(self.tmp)

		eqry = str(query) + str(geo_col) + str(crs)
		savePath = self.tmp / (str(hashlib.md5(eqry.encode('UTF8')).hexdigest()) + '.fthr')
		loadedFromServer = False

		# Load
		if storeIt and os.path.exists(savePath) and not force_refetch:
			df = _loader(savePath)  # type: ignore
		else:
			df = pd.read_sql(query, self._engine, params=params)
			loadedFromServer = True

		# Parse
		if loadedFromServer and geo_col is not None:
			gk = pdg.GeoSeries.from_wkb(df[geo_col])  #  type: ignore
			df = pdg.GeoDataFrame(df, geometry=gk, crs=crs)
			df = df.drop([geo_col], axis=1)  # type: ignore


		# Save
		if storeIt and not df.empty:
			df.to_feather(str(savePath))
		
		if df.empty:
			logging.warn(f'The dataframe from the following query was empty\n{query}')

		return df
