sync-version:
    #!/bin/bash -e
    cd {{justfile_directory()}}
    vers='0.1.0-dev'
    for pkg in `ls crates`
    do
        echo $pkg $vers
        pushd crates/$pkg > /dev/null
        cargo set-version $vers
        popd > /dev/null
    done

fmt:
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    cargo fmt
    cargo sort -w > /dev/null

build:
    mold -run cargo build --release --offline

miri:
    MIRIFLAGS=-Zmiri-backtrace=full \
    cargo miri test -p epkv-utils -- --nocapture --test-threads=1 \
        --skip watermark \
        --skip flag_group

test:
    mold -run cargo test --release --offline

check: fmt
    cargo check
    cargo clippy

dev:
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    just check
    just miri
    just test

udeps:
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    cargo +nightly udeps
