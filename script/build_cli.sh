#!/bin/bash
#
# Utility script for building CLI tarballs for a given release.
#
# Usage: ./scripts/build_cli.sh <tag>
#
# Arguments:
#   tag (required) - Release tag (eg. v2.1.2).
#
# Example:
#   $ ./scripts/build_cli.sh v2.1.2
#
# Description:
#
#   Two tarballs will be created for the CLI, one for
#   Darwin (OSX) and the other for Linux. Both binaries are built using
#   the `cli` Make target which will automatically bake the tag, commit SHA
#   and timestamp into the associated binary. The resulting packages will
#   can be found ./out/dosa-{darwin,linux}-{tag}.tar.gz.
#
###############################################################################


usage() {
    echo "Usage: ./build_cli.sh <tag>"
    echo
    echo "Arguments:"
    echo "  tag (required) - Release tag."
    echo
}

main() {
    # validate params
    local tag="$1"
    [[ -z "${tag}" ]] && usage && exit

    echo "Building CLI for ${tag}..."

    # build both of the binaries
    target=Darwin make cli || ( echo "Darwin build failed" && exit )
    target=Linux make cli || ( echo "Linux build failed" && exit )

    # ensure both binaries exist
    [[ -f ./out/cli/darwin/dosa ]] || ( echo "Darwin binary not found" && exit )
    [[ -f ./out/cli/linux/dosa ]] || ( echo "Linux binary not found" && exit )

    # create tmpdir
    mkdir -p ./out/dosa-{darwin,linux}

    # copy binaries to tmpdirs
    cp ./out/cli/darwin/dosa ./out/dosa-darwin/
    cp ./out/cli/linux/dosa ./out/dosa-linux/

    # create packages
    tar -pzcf ./out/dosa-darwin-${tag}.tar.gz -C ./out dosa-darwin || ( echo "failed to create darwin tarball" && exit )
    tar -pzcf ./out/dosa-linux-${tag}.tar.gz -C ./out dosa-linux || ( echo "failed to create linux tarball" && exit )

    # ensure packages were built correctly
    mkdir ./out/tmp
    tar -xzf ./out/dosa-darwin-${tag}.tar.gz -C ./out/tmp || ( echo "failed to extract darwin tarball" && exit )
    tar -xzf ./out/dosa-linux-${tag}.tar.gz -C ./out/tmp || ( echo "failed to extract linux tarball" && exit )

    # and can be extracted correctly
    [[ -f ./out/tmp/dosa-darwin/dosa ]] || ( echo "package was not built correctly: could not find extracted path for Darwin (./out/tmp/dosa-darwin/dosa)." && exit )
    [[ -f ./out/tmp/dosa-linux/dosa ]] || ( echo "package was not built correctly: could not find extracted path for Linux (./out/tmp/dosa-darwin/dosa)." && exit )

    # done, cleanup
    rm -rf ./out/dosa-darwin ./out/dosa-linux ./out/tmp

    echo "DONE"
    echo "Darwin package built: ./out/dosa-darwin-${tag}.tar.gz"
    echo "Linux package built: ./out/dosa-linux-${tag}.tar.gz"
}

main "$@"
