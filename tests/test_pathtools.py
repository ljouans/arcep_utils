import os
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

import utils.pathtools as pth




def test__get_tool_path():
    assert pth._get_tool_path().is_dir()
    assert pth._get_tool_path().exists()
    assert pth._get_tool_path().stem == 'utils'


@pytest.mark.usefixtures('mocker')
@pytest.mark.parametrize('func,folder_to_find', [
    (pth.data_path, 'data'),
    (pth.tmp_path, 'data/tmp'),
    (pth.prod_path, 'data/prod'),
    (pth.config_path, 'config'),
])
def test_data_path(mocker: MockerFixture, func, folder_to_find):
    this_folder_path = Path(os.path.realpath(__file__)).parent
    mocker.patch('utils.pathtools.Path.parent',
                 new_callable=mocker.PropertyMock(return_value=this_folder_path / 'test_trash_dir'))

    # path = pth.data_path()
    path = func()
    assert Path((this_folder_path / 'test_trash_dir') / folder_to_find).exists()


