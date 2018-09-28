__author__ = 'Federico Schmidt'

import os
import subprocess
from core.lib.io.file import create_folder_with_permissions
from core.lib.xplatform.cmd import print_same_line


def run_boot_hook(hook_path):
    cmd = "sh \"%s\"" % hook_path

    try:
        print_same_line("Running boot hook...")
        # We try running the hook.
        output = subprocess.check_output(cmd, shell=True, env=os.environ)
        print(" [OK]")
        # Print hook's output with '>' at the start of every line.
        print('> ' + output.replace('\n', '\n> '))
    except Exception as ex:
        # If an Exception pops up then we halt the system's boot process.
        print_same_line(" [ERROR]\n")
        raise RuntimeError("An Exception ocurred while running the boot hook: \"" + str(ex).strip() + "\".")
    return


def boot_system(config):

    root_path = config.root_path

    paths = config.get('paths')

    if not paths:
        raise RuntimeError('Missing paths dictionary in system configuration file.')

    # Get the configured temp folder or retrieve a default one.
    tmp_folder = paths.get('temp_folder', './.tmp')

    # Check that the path is a string.
    if not isinstance(tmp_folder, str):
        print('[ERROR] temp_folder in config should be a string.')
        exit(1)

    # Make it relative to the root path.
    tmp_folder = os.path.join(root_path, tmp_folder)

    rundir = paths.get('rundir', 'rundir')

    if not os.path.isabs(rundir):
        rundir = os.path.join(root_path, rundir)

    create_folder_with_permissions(rundir)
    config.rundir = rundir

    create_folder_with_permissions(tmp_folder)
    config.temp_folder = tmp_folder

    boot_hook = os.path.join(root_path, 'hooks', 'boot.sh')

    # If the boot hook file exists and the software has run permissions then we call it.
    if os.path.isfile(boot_hook) and os.access(boot_hook, os.X_OK):
        run_boot_hook(boot_hook)
    return
