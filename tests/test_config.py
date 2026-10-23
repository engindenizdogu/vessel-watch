from pipeline.config import ROOT, load_config


def test_paths_resolve_under_repo():
    cfg = load_config()
    for p in cfg["paths"].values():
        assert p.is_absolute()
        assert ROOT in p.parents


def test_harbor_bbox_is_ordered():
    box = load_config()["harbor_bbox"]
    assert box["lat_min"] < box["lat_max"]
    assert box["lon_min"] < box["lon_max"]
