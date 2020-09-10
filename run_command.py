"""Run a terminal command."""

import subprocess

def run_command(cmd, print_output=True):
    try:
        output = subprocess.check_output(
            cmd, stderr=subprocess.STDOUT, shell=True,
            universal_newlines=True, start_new_session=True)
    except subprocess.CalledProcessError as exc:
        print("Status : FAIL", exc.returncode, exc.output)
    else:
        if print_output:
            print("Output: \n{}\n".format(output))
