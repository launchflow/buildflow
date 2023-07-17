COPY_TEMPLATE = """\
#!/bin/bash
FROM_PATH="$PWD/{copy_from}"
TO_PATH="$BUILD_WORKSPACE_DIRECTORY/{to}/"
echo "Copying from $FROM_PATH to $TO_PATH"
cp -f $FROM_PATH $TO_PATH
"""

def _copy_to_workspace_impl(ctx):
    copy_file = ctx.file.file
    copy_runfile_path = copy_file.short_path
    copy_to = ctx.attr.file.label.package + ctx.attr.output_dir
    script = ctx.actions.declare_file(ctx.label.name)
    script_content = COPY_TEMPLATE.format(
        copy_from = copy_runfile_path,
        to = copy_to,
    )

    ctx.actions.write(script, script_content, is_executable = True)
    runfiles = ctx.runfiles(files = [copy_file])

    return [DefaultInfo(executable = script, runfiles = runfiles)]

copy_to_workspace = rule(
    implementation = _copy_to_workspace_impl,
    attrs = {
        "file": attr.label(allow_single_file = True),
        "output_dir": attr.string(mandatory = False),
    },
    executable = True,
)
