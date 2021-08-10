import hashlib
import logging
import os
import shutil
from pathlib import Path

import pandas as pd
import pytest
from pytest_mock import MockerFixture

import utils.pathtools as pth
from utils.argstruct.database_secret import ExtendedDatabaseSecret
from utils.argstruct.geo_table_info import GeoInfo
from utils.dbtool import Tool
from utils.dbtool import _connection_string_from_db_secret
from utils.dbtool import _connection_string_from_secret_file
from utils.dbtool import _rm_string_marker


def test__connection_string_from_secret_file():
    db_secret = ExtendedDatabaseSecret(user='1', password='2', host='3', port='4', db='5')
    assert _connection_string_from_db_secret(db_secret) == 'postgresql://1:2@3:4/5'


@pytest.fixture
def this_folder():
    return Path(os.path.realpath(__file__)).parent


def test__connection_string_from_db_secret__find_content_from_user_path(this_folder: Path):
    assert _connection_string_from_secret_file(this_folder / 'test_files/config.cfg') == \
           'this_is_totally_a_connection_string'


def test__connection_string_from_db_secret__find_content_from_default_path(mocker: MockerFixture, this_folder: Path):
    mocker.patch('utils.pathtools.get_tool_path', return_value=this_folder / 'test_files')
    assert _connection_string_from_secret_file() == 'this_is_totally_an_other_connection_string'


@pytest.mark.parametrize('string,expected', [
    ('"text"', "text"),
    ('\'text\'', "text"),
    ('\'text', "'text"),
    ('text\'', "text'"),
    ('"text\'', "\"text'"),
    ('text', 'text'),
    ])
def test__rm_string_marker(string, expected):
    assert _rm_string_marker(string) == expected


@pytest.mark.usefixtures('mocker')
@pytest.mark.parametrize('connstring,dbsecret,secretfile,expected', [
    ('Connection String', None, None, 'Connection String'),
    (None, ExtendedDatabaseSecret(user='1', password='2', host='3', port='4', db='5'), None, 'postgresql://1:2@3:4/5'),
    ('Connection String', ExtendedDatabaseSecret(user='1', password='2', host='3', port='4', db='5'), None,
     'Connection String'),
    (None, None, 'Secret File', 'Secret File'),
    ('Connection String', ExtendedDatabaseSecret(user='1', password='2', host='3', port='4', db='5'), 'Secret File',
     'Connection String'),
    ])
def test__create_engine__with_string(mocker, connstring, dbsecret, secretfile, expected):
    mocker.patch('utils.dbtool.create_engine', new=lambda x: x)
    mocker.patch('utils.dbtool._connection_string_from_secret_file', new=lambda x: x)
    tool = Tool()
    not_engine = tool._create_engine(connection_string=connstring, database_secret=dbsecret,
                                     secret_path_file=secretfile)
    assert not_engine == expected


@pytest.mark.skip(reason='Trop lent. À lancer de temps en temps. Requiert Putty.')
def test__create_engine__real_connexion():
    tool = Tool(secret_path_file=pth.get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    tool.engine.connect()
    assert True


@pytest.mark.skip(reason='Trop lent. À lancer de temps en temps. Requiert Putty.')
def test_has_table():
    tool = Tool(secret_path_file=pth.get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    assert tool.has_table(table='immeuble', schema='base_infra')
    assert not tool.has_table(table='pas_immeuble', schema='base_infra')


@pytest.mark.skip(reason='Trop lent. À lancer de temps en temps. Requiert Putty.')
def test__get_crs():
    tool = Tool(secret_path_file=pth.get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    geo_info = GeoInfo(table_path='base_infra.immeuble', column='geom', condition="code_insee = '71378'")
    crs = tool._get_crs(geo_info)
    assert crs == '2154'

    geo_info = GeoInfo(table_path='base_infra.immeuble', column='geom', condition="code_insee = '97410'")
    crs = tool._get_crs(geo_info)
    assert crs == '2975'


@pytest.fixture
def local_tmp_path():
    local_tmp = pth.get_tool_path() / 'tests/tmp'
    local_tmp.mkdir(parents=True, exist_ok=True)

    yield local_tmp

    shutil.rmtree(local_tmp)


def test_fetch_query__warning_empty_df(mocker: MockerFixture, local_tmp_path: Path, caplog):
    nop = lambda *x: ()

    caplog.set_level(logging.WARNING)
    mocker.patch('utils.dbtool.pth.tmp_path', return_value=local_tmp_path)
    mocker.patch('utils.dbtool.Tool._create_engine', new=nop)
    mocker.patch('utils.dbtool._create_dir', new=nop)
    mocker.patch('utils.dbtool.pd.read_sql', new=mocker.MagicMock(return_value=pd.DataFrame().reset_index()))

    tool = Tool()
    df = tool.fetch_query(query='totally a query')
    assert df.empty
    assert 'from the following query was empty' in caplog.text


def test_fetch_query__saving(mocker: MockerFixture, local_tmp_path: Path):
    mocker.patch('utils.dbtool.pth.tmp_path', return_value=lambda *x: x[0])
    mocker.patch('utils.dbtool.Tool._create_engine', new=lambda *x: x[0])
    mocker.patch('utils.dbtool._create_dir', new=lambda x: ())
    read_sql_mock = mocker.patch('utils.dbtool.pd.read_sql',
                                 new=mocker.MagicMock(return_value=pd.DataFrame(data={'a': [1, 2, 3]})))

    tool = Tool()
    df = tool.fetch_query(query='totally a query')
    eqry = 'totally a query' + str(None) + str(None)
    save_path = local_tmp_path / (str(hashlib.md5(eqry.encode("UTF8")).hexdigest()) + ".fthr")
    assert save_path.exists()
    assert df.shape == (3, 1)
    assert len(read_sql_mock.mock_calls) == 1

    df2 = tool.fetch_query(query='totally a query')
    assert (df == df2)['a'].all()
    # No new call
    assert len(read_sql_mock.mock_calls) == 1


# @pytest.mark.skip(reason="Lent. À lancer de temps en temps. Requiert la connexion Putty")
def test_fetch_query__geopandas(mocker: MockerFixture, local_tmp_path):
    mocker.patch('utils.dbtool.pth.tmp_path', return_value=local_tmp_path)
    tool = Tool(secret_path_file=pth.get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    geo_info = GeoInfo(table_path='base_infra.immeuble', column='geom', condition="code_insee = '71378'")
    df = tool.fetch_query(query='''SELECT * FROM base_infra.immeuble where code_insee = '71378' limit 97''',
                          geo_info=geo_info)
    assert 'geometry' in df.columns
    assert df.shape == (97, 20)
    assert df.crs.to_epsg() == 2154


@pytest.mark.skip(reason="Lent. À lancer de temps en temps. Requiert la connexion Putty")
def test_fetch_query__pandas(mocker, local_tmp_path):
    mocker.patch('utils.dbtool.pth.tmp_path', return_value=local_tmp_path)
    tool = Tool(secret_path_file=pth.get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    df = tool.fetch_query(query='''SELECT * FROM base_infra.operateurs LIMIT 97''')
    assert df.shape == (97, 4)
    assert df[df['code_2d'] == 'DB'].code_l33_13.values[0] == 'DLFE'


@pytest.mark.skip(reason='Requiert Putty.')
def test_drop_table__integration():
    tool = Tool(secret_path_file=pth.get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    tool.fetch_query(query='DROP TABLE IF EXISTS loic._test_to_delete;'
                           'create table loic._test_to_delete (dummy INTEGER NOT NULL PRIMARY KEY); '
                           'select 1 from loic._test_to_delete limit 0;')

    assert tool.has_table(table='_test_to_delete', schema='loic')
    tool.drop_table('_test_to_delete', schema='loic')
    assert not tool.has_table(table='_test_to_delete', schema='loic')
