import logging
import zipfile
from pathlib import Path
from typing import IO, List, Optional, Union

import pandas as pd
import tqdm

import utils.pathtools as pth
from utils import misc

logger = logging.getLogger(__name__)


def _read_single_ipe_file(filelike: IO[bytes], cols: Optional[List[str]] = None,
                          nrows: Optional[int] = None) -> pd.DataFrame:
    all_encodings = ['UTF-8', 'Windows-1252', 'Latin-1']

    df = pd.DataFrame()
    for encoding in all_encodings:
        try:
            take_col = lambda c: c in cols
            df = pd.read_csv(filelike, sep=';', decimal=',', encoding=encoding, usecols=take_col, dtype=str,
                             nrows=nrows)  # type: ignore

            if df.shape[1] > 0:
                break

            logger.debug('Not enough columns with encoding %s. Trying with some other.', encoding)

        except UnicodeDecodeError:
            logger.debug('Encoding %s did not work. Trying anew', encoding)
        filelike.seek(0)
    return df


def _type_df(df: pd.DataFrame, numeric_cols: List[str] = []):
    for col in numeric_cols:
        df.loc[:, col] = pd.to_numeric(df[col], errors='coerce')


def parse_ipe(ipe_zip_path: Union[str, Path], columns: List[str], numeric_cols: List[str] = None,
              cols_are_optional: bool = True, _test_nrows: int = None):
    with zipfile.ZipFile(ipe_zip_path) as z:
        file_issues = []
        dfs = []

        for name in misc.make_iterator(z.namelist(), low_bound=1, desc='Reading IPE'):
            extension = name[-3:]
            if extension == 'csv':
                with z.open(name, 'r') as f:
                    df = _read_single_ipe_file(f, cols=columns, nrows=_test_nrows)

                    has_all_cols = df.shape[1] == len(columns)
                    if not has_all_cols and not cols_are_optional:
                        file_issues.append(name)

                    if has_all_cols or cols_are_optional:
                        dfs.append(df)
        if len(file_issues) > 0:
            logger.debug('Done reading. Had %s issues. Could not read files : %s', len(file_issues), file_issues)

    df_full = pd.concat(dfs)  # type: ignore
    df_full: pd.DataFrame

    numeric_cols = [] if numeric_cols is None else numeric_cols
    _type_df(df_full, numeric_cols=numeric_cols)
    df_full = df_full.reset_index()[columns]

    return df_full


def _read_all_ipe(columns: List[str], numeric_cols: List[str] = None, cols_are_optional: bool = True,
                  _test_nrows: int = None):
    """
    Lis tous les IPE de l'archive pointée.
    - columns: liste des colonnes à charger
    - numeric_cols: liste des colonnes numériques. Doit aussi apparaitre dans columns
    - cols_are_optional: Si vrai (par défaut), le fichier est chargé même s'il manque des colonnes. Si faux, ne charge pas les fichiers où il manque des colonnes.

    Le scripts notifie l'utilisateur des fichiers où il a eu des problèmes de chargement.
    Cela inclus 
    - les cas où il manque des colonnes parmis celles spécifiées
    - Les cas où le CSV n'était pas lisible
    - Les cas où il n'a pas réussi a trouver le formattage
    """
    logging.basicConfig(level=logging.INFO)
    with zipfile.ZipFile(pth.specific_datapath('ipe') / 'IPE_t1_2021_corrige.zip') as z:
        file_issues = []
        dfs = []
        for name in tqdm.tqdm(z.namelist(), desc='Reading IPE'):
            # name='IPE_t1_2021/IPE_Forbach_FIBA_PM_IPEZMD_V30_20210419.csv'
            # for name in z.namelist():
            ext = name[-3:]
            if ext == 'csv':
                with z.open(name, 'r') as f:
                    df = _read_single_ipe_file(f, cols=columns, nrows=_test_nrows)
                    # dfs[-1] = dfs[-1].drop_duplicates(subset='ReferencePM')
                    has_all_cols = df.shape[1] == len(columns)
                    if not has_all_cols:
                        file_issues.append(name)
                    if has_all_cols or cols_are_optional:
                        dfs.append(df)
        logging.info('Done reading. Had %s issues. Could not read files : %s', len(file_issues), file_issues)

    df_full = pd.concat(dfs)  # type: ignore
    df_full: pd.DataFrame

    numeric_cols = [] if numeric_cols is None else numeric_cols
    _type_df(df_full, numeric_cols=numeric_cols)
    df_full = df_full.reset_index()[columns]

    return df_full


def read_single_ipe(ipe_name: str, columns: List[str], numeric_cols: List[str] = None,
                    zipfilepath: Optional[Union[str, Path]] = None, _test_nrows: int = None):
    """Lis un IPE spécifique dans l'archive IPE_t1_2021_corrige.zip si aucune autre n'est spécifiée.

    Args:
        ipe_name (str): nom du fichier à lire
        columns (List[str]): Colonnes à garder
        numeric_cols (List[str], optional): Colonnes contenant des nombres. Defaults to None.
        zipfilepath (Optional[Union[str, Path]], optional): chemin vers une archive autre que 'IPE_t1_2021_corrige.zip. Defaults to None.
        _test_nrows (int, optional): Valeur de test. Limite la lecture de l'IPE à _test_nrows lignes. Defaults to None.

    Returns:
        [type]: [description]
    """
    if zipfilepath is None:
        zipfilepath = pth.specific_datapath('ipe') / 'IPE_t1_2021_corrige.zip'
    zipfilepath = Path(zipfilepath)

    with zipfile.ZipFile(zipfilepath) as z:
        with z.open(f'IPE_t1_2021/{ipe_name}', 'r') as f:
            df = _read_single_ipe_file(f, cols=columns, nrows=_test_nrows)

    numeric_cols = [] if numeric_cols is None else numeric_cols
    _type_df(df, numeric_cols=numeric_cols)
    return df


def stem_to_interop(stem: str) -> str:
    return stem.split('_')[2]


if __name__ == '__main__':
    df = read_all_ipe(columns=['CodeInseeImmeuble', 'EtatPM', 'EtatImmeuble', 'NombreLogementsAdresseIPE', 'TypePBO',
                               'TypeRaccoPBPTO', 'CodeOI'],
                      numeric_cols=['NombreLogementsAdresseIPE'])
