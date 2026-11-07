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



def test_gates_are_lines_inside_the_harbor_box():
    cfg = load_config()
    box = cfg["harbor_bbox"]
    names = [g["name"] for g in cfg["gates"]["lines"]]
    assert len(names) == len(set(names))
    for gate in cfg["gates"]["lines"]:
        assert len(gate["line"]) == 2 and gate["label"]
        for lon, lat in gate["line"] + [gate["inside"]]:
            assert box["lon_min"] < lon < box["lon_max"] and box["lat_min"] < lat < box["lat_max"]
