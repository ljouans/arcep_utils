import json
from typing import Dict

import utils.pathtools as pth
import json
from typing import Dict

import utils.pathtools as pth


def read_json_config() -> Dict:
    json_base = None
    with open(pth.outer_out_path() / 'base.cfg', 'r') as f:
        json_base = json.load(f)
    json_run = None
    with open(pth.outer_out_path() / 'run.cfg', 'r') as f:
        json_run = json.load(f)

    return _merge(json_base, json_run)


def _merge(base, run):
    if not isinstance(base, dict):
        return run

    shared_keys = set(base).intersection(run)
    new_keys = set(run).difference(shared_keys)

    for k in shared_keys:
        base[k] = _merge(base[k], run[k])

    for k in new_keys:
        base[k] = run[k]
    return base
