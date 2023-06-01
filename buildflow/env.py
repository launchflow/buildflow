import os


class LOCAL:
    num_cpus = os.cpu_count()


class USER:
    user = os.environ.get("USER", None)
    home = os.environ.get("HOME", None)
