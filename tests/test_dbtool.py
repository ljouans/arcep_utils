import os
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from utils.argstruct.database_secret import ExtendedDatabaseSecret
from utils.dbtool import _connection_string_from_db_secret, _connection_string_from_secret_file, _rm_string_marker, Tool


def test_has_table():
    assert False


def test__get_crs():
    assert False


def test_fetch_query():
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
    not_engine = tool._create_engine(connection_string=connstring, database_secret=dbsecret, secret_path_file=secretfile)
    assert not_engine == expected

