import re
import sys

with open("src/f4/Utilities.py") as in_file:
    version = re.findall(r"return \"(\d\.\d\.\d)\"", in_file.read())

    if len(version) != 1:
        print(f"Invalid version in Utilities.py!")
        sys.exit(1)

    version = version[0]

with open("setup.cfg") as in_file:
    setup_text = in_file.read()
    setup_version = re.findall(r"version = (\d\.\d\.\d)", setup_text)

    if len(setup_version) != 1:
        print(f"Invalid version in setup.cfg!")
        sys.exit(1)

    setup_version = setup_version[0]

if version == setup_version:
    print(f"The version is identical in Utilities.py and setup.cfg!")
    sys.exit(1)

with open("setup.cfg", "w") as out_file:
    print(f"Updating version in setup.cfg to {version}!")
    setup_text = setup_text.replace(setup_version, version)
    out_file.write(setup_text)

print("Done!")
