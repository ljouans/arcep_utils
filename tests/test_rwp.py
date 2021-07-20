import utils.pathtools as pth
from utils.ipe.rwp import parse_ipe


def test_parse_ipe__integration():
    ipe_zip_path = pth.specific_datapath('ipe') / 'IPE_t1_2021_corrige.zip'

    df = parse_ipe(ipe_zip_path=ipe_zip_path, columns=['IdentifiantImmeuble', 'CodeAdresseImmeuble'], _test_nrows=20)
    assert df.shape == (23119, 2)
