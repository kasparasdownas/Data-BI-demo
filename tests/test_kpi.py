from src.kpi import kpi_arpu

def test_kpi_arpu():
    assert kpi_arpu(1000, 100) == 10
    assert kpi_arpu(0, 0) == 0.0
