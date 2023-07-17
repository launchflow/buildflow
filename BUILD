load("@com_github_grpc_grpc//bazel:cython_library.bzl", "pyx_library")
load("//:defs.bzl", "copy_to_workspace")

package(default_visibility = ["//visibility:private"])

copy_to_workspace(
    name = "cp_pubsub_so",
    file = "buildflow/core/io/gcp/strategies/_cython/pubsub_source.so",
    output_dir = "buildflow/core/io/gcp/strategies/_cython",
)

pyx_library(
    name = "cython_pubsub_source",
    srcs = [
        "buildflow/core/io/gcp/strategies/_cython/pubsub_source.pyx",
    ],
    deps = [
        ":c_pubsub",
    ],
)

cc_library(
    name = "c_pubsub",
    srcs = [
        "buildflow/core/io/gcp/strategies/_cython/pubsub_stream.cpp",
    ],
    hdrs = [
        "buildflow/core/io/gcp/strategies/_cython/pubsub_stream.h",
    ],
    deps = [
        "@google_cloud_cpp//:pubsub",
    ],
)
