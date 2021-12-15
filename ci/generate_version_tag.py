"""Version Tag Generator

This script is designed to parse a list of commits written according to the
conventional commit guidelines https://www.conventionalcommits.org/en/v1.0.0/.
Based on the type of each commit either a patch or a minor version is updated
from the original tag provided with respect to semantic versioning
https://semver.org/.

If a script encounters a breaking change in the changelog it will throw an error
in order to make sure major releases are expected and explicit. There is a parameter
passed to the script to explicitly trigger a major version increase.

Prerelease tags (e.g. -alpha) are passed manually, if required by the build state.
They will be appended to the next incremented version. A run when the previous version
provided was a pre-release would finalize it e.g. (1.0.1-alpha -> 1.0.1)
"""

import argparse
from typing import Tuple

import semver


def main(changes: str, old_tag: str, prerelease_tag: str, major_release: bool):
    """
    Generate new version tag.

    Args:
        changes: list of newline-separated change titles
        old_tag: previous version tag e.g. 1.0.1
        prerelease_tag: add a pre-relrease tag to the next version e.g -alpha
        major_release: boolean to override and trigger a major release
    """
    ver = semver.VersionInfo.parse(old_tag)
    if major_release:
        new_ver = ver.bump_major()
    else:
        if not changes:
            raise Exception("No changes since the last release!")
        # Breaking changes are of form <tag>!: <description>
        # To avoid automatically publishing major version they need to be
        # explicitly specified
        breaking = [c for c in changes.split("\n") if c.split(":", 1)[0].endswith("!")]
        if breaking:
            raise Exception(
                "Breaking changes detected, please make sure "
                "to provide an override for this."
            )

        # If previous version contained a pre-release tag
        if ver.prerelease:
            new_ver = ver.finalize_version()
        else:
            # Enhancements bump minor version while anything else is considered a patch
            enhancements = [c for c in changes.split("\n") if c.startswith("feat:")]
            new_ver = ver.bump_minor() if enhancements else ver.bump_patch()
    if prerelease_tag:
        new_ver = new_ver.bump_prerelease(prerelease_tag)
    print(new_ver)


def parse_arguments() -> Tuple[str, str, str, bool]:
    parser = argparse.ArgumentParser(
        description="Determine the next package version based on conventional commits."
    )
    parser.add_argument("changes", help="Output of git log -pretty=format:%s")
    parser.add_argument("old_tag", help="Previous semantic version tag")
    parser.add_argument("--prerelease_tag", help="For alpha and pre-releases")
    parser.add_argument("--major_release", help="Override and bump the major version")
    args = parser.parse_args()
    return args.changes, args.old_tag, args.prerelease_tag, bool(args.major_release)


if __name__ == "__main__":
    changes, old_tag, prerelease_tag, major_release = parse_arguments()
    main(changes, old_tag, prerelease_tag, major_release)
