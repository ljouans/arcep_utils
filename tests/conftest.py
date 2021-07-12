import os
import pathlib
import shutil

import pytest
import _pytest.skipping


def pytest_addoption(parser):
    parser.addoption(
        "--no-skips",
        action="store_true",
        default=False, help="disable skip marks")


@pytest.hookimpl(tryfirst=True)
def pytest_cmdline_preparse(config, args):
    if "--no-skips" not in args:
        return

    def no_skip(*args, **kwargs):
        return

    _pytest.skipping.skip = no_skip


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.

    Creates the config file used for test generation
    """
    this_folder = pathlib.Path(os.path.realpath(__file__)).parent
    (this_folder / 'test_trash_dir').mkdir(parents=True, exist_ok=True)


def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """

    this_folder = pathlib.Path(os.path.realpath(__file__)).parent
    shutil.rmtree(this_folder / 'test_trash_dir')
