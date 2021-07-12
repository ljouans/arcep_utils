import os
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

import utils.pathtools as pth
from utils.argstruct.database_secret import ExtendedDatabaseSecret
from utils.dbtool import _connection_string_from_db_secret, _connection_string_from_secret_file, _rm_string_marker, Tool


def test__get_crs():
    assert False


def test__connection_string_from_secret_file():
    db_secret = ExtendedDatabaseSecret(user='1', password='2', host='3', port='4', db='5')
    assert _connection_string_from_db_secret(db_secret) == 'postgresql://1:2@3:4/5'


@pytest.fixture
def this_folder():
    return Path(os.path.realpath(__file__)).parent


def test__connection_string_from_db_secret__find_content_from_user_path(this_folder: Path):
    assert _connection_string_from_secret_file(this_folder) == 'this_is_totally_a_connection_string'


def test__connection_string_from_db_secret__find_content_from_default_path(mocker: MockerFixture, this_folder: Path):
    mocker.patch('utils.pathtools._get_tool_path', return_value=this_folder / 'test_files')
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
    nop = lambda x: x
    mocker.patch('utils.dbtool.create_engine', new=nop)
    mocker.patch('utils.dbtool._connection_string_from_secret_file', new=nop)
    tool = Tool()
    not_engine = tool._create_engine(connection_string=connstring, database_secret=dbsecret,
                                     secret_path_file=secretfile)
    assert not_engine == expected


@pytest.mark.skip(reason='Trop lent. À lancer de temps en temps. Requiert Putty.')
def test__create_engine__real_connexion():
    tool = Tool(secretPathFile=pth._get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    tool.engine.connect()
    assert True


@pytest.mark.skip(reason='Trop lent. À lancer de temps en temps. Requiert Putty.')
def test_has_table():
    tool = Tool(secretPathFile=pth._get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    assert tool.has_table(table='immeuble', schema='base_infra')
    assert not tool.has_table(table='pas_immeuble', schema='base_infra')


@pytest.mark.skip(reason='Trop lent. À lancer de temps en temps. Requiert Putty.')
def test__get_crs():
    tool = Tool(secretPathFile=pth._get_tool_path() / 'tests/test_files/actually_secret/db.cfg')
    crs = tool._get_crs(table='immeuble', geo_col='geom', schema='base_infra', condition="code_insee = '71378'")
    assert crs == '2154'

    crs = tool._get_crs(table='immeuble', geo_col='geom', schema='base_infra', condition="code_insee = '97410'")
    assert crs == '2975'


def test_fetch_query():
    assert False
