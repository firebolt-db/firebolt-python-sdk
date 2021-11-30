import argparse

import semver


def main(changes, old_tag, prerelease_tag, major_release):
    ver = semver.VersionInfo.parse(old_tag)
    if major_release:
        new_ver = ver.bump_major()
    elif prerelease_tag:
        new_ver = ver.bump_prerelease(prerelease_tag)
    else:
        if not changes:
            raise Exception("No changes since the last release!")
        # Breaking changes are of form <tag>!: <description>
        # To avoid automatically publishing major version they need to be
        # explicitly specified
        breaking = [c for c in changes.split("\n") if "!:" in c]
        if breaking:
            raise Exception(
                "Breaking changes detected, please make sure "
                "to provide an override for this."
            )

        if ver.prerelease:
            new_ver = ver.finalize_version()
        else:
            # Enhancements bump minor version while anything else is considered a patch
            enhancements = [c for c in changes.split("\n") if c.startswith("enh:")]
            new_ver = ver.bump_minor() if enhancements else ver.bump_patch()
    print(new_ver)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Determine the next tag based on change tags"
    )
    parser.add_argument("changes", help="Output of git log -pretty=format:%s")
    parser.add_argument("old_tag", help="Previous semantic version tag")
    parser.add_argument("--prerelease_tag", help="For alpha and pre-releases")
    parser.add_argument("--major_release", help="Override and bump the major version")
    args = parser.parse_args()
    return args.changes, args.old_tag, args.prerelease_tag, args.major_release


if __name__ == "__main__":
    changes, old_tag, prerelease_tag, major_release = parse_arguments()
    main(changes, old_tag, prerelease_tag, major_release)
