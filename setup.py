import os
import subprocess
import sys
from typing import List


ROOT_DIR = os.path.dirname(__file__)


# Calls Bazel in PATH, falling back to the standard user installation path
# (~/bin/bazel) if it isn't found.
def run_bazel(cmd_args: List[str], *args, **kwargs):
    home = os.path.expanduser("~")
    first_candidate = os.getenv("BAZEL_PATH", "bazel")
    candidates = [first_candidate]
    candidates.append(os.path.join(home, "bin", "bazel"))
    result = None
    for i, cmd in enumerate(candidates):
        try:
            result = subprocess.check_call([cmd] + cmd_args, *args, **kwargs)
            break
        except IOError:
            if i >= len(candidates) - 1:
                raise
    return result


def build():
    bazel_env = dict(os.environ, PYTHON3_BIN_PATH=sys.executable)

    run_bazel(
        ["build", "//:cython_pubsub_source"],
        env=bazel_env,
    )
    run_bazel(
        ["run", "//:cp_pubsub_so"],
        env=bazel_env,
    )


if __name__ == "__main__":
    import setuptools
    import setuptools.command.build_ext

    class build_ext(setuptools.command.build_ext.build_ext):
        def run(self):
            build()

    class BinaryDistribution(setuptools.Distribution):
        def has_ext_modules(self):
            return True


setuptools.setup(
    name="buildflow",
    version="0.1.2",
    python_requires=">=3.7",
    author="LaunchFlow",
    author_email="founders@launchflow.com",
    description="Build your entire system in minutes using Python.",  # noqa: E501
    long_description=open(
        os.path.join(ROOT_DIR, "README.md"), "r", encoding="utf-8"
    ).read(),
    url="http://www.buildflow.dev",
    classifiers=[
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    packages=["buildflow"],
    cmdclass={"build_ext": build_ext},
    distclass=BinaryDistribution,
    entry_points={"console_scripts": ["buildflow = buildflow.cli.main:main"]},
    setup_requires=["cython >= 0.29.32", "wheel"],
    install_requires=[
        # TODO: split up AWS and GCP dependencies.
        "boto3",
        "dacite",
        "duckdb==0.6.0",
        "google-auth",
        "google-cloud-bigquery",
        "google-cloud-bigquery-storage",
        "google-cloud-monitoring",
        "google-cloud-pubsub",
        "google-cloud-storage",
        # TODO: https://github.com/grpc/grpc/issues/31885
        "grpcio<1.51.1",
        "fastparquet",
        "opentelemetry-api",
        "opentelemetry-sdk",
        "opentelemetry-exporter-otlp",
        "opentelemetry-exporter-jaeger",
        "pandas",
        "pulumi==3.35.3",
        "pulumi_gcp",
        "pyarrow",
        "pydantic < 2.0.2",
        "pyyaml",
        "ray[default] > 2.0.0",
        "ray[serve] > 2.0.0",
        "typer",
        "redis",
    ],
    extras_require={
        "def": [
            "moto",
            "pytest",
            "pytest-cov",
            "pytest-timeout",
            "ruff",
            "black",
            "pre-commit",
            "setuptools",
            "wheel",
        ]
    },
)
