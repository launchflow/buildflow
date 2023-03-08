def pytest_addoption(parser):
    parser.addoption('--disable_usage_stats',
                     action='store_true',
                     default=False)
