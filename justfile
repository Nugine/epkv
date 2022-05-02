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
    cargo fmt
    cargo sort -w > /dev/null

build:
    mold -run cargo build --release --offline

miri:
    MIRIFLAGS=-Zmiri-backtrace=full \
    cargo miri test -p epkv-utils -- --nocapture --test-threads=1 \
        --skip watermark \
        --skip flag_group \
        --skip stepper

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

doc:
    cargo doc -p rocksdb --no-deps
    cargo doc --workspace --no-deps --open

generate-local-cluster: build
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    export RUST_BACKTRACE=full
    ./target/release/epkv-eval cluster generate \
        --config crates/epkv-eval/tests/local-cluster.json \
        --target /tmp/epkv-cluster/config

local-server name: build
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    export RUST_BACKTRACE=full
    export RUST_LOG=epkv_server=debug,epkv_rocks=debug,epkv_epaxos=debug,epkv_protocol=debug
    ./target/release/epkv-server --config /tmp/epkv-cluster/config/{{name}}.json

local-monitor: build
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    export RUST_BACKTRACE=full
    export RUST_LOG=epkv_monitor=debug,epkv_protocol=debug
    ./target/release/epkv-monitor --config /tmp/epkv-cluster/config/monitor.json

eval *ARGS:
    #!/bin/bash -e
    cd {{justfile_directory()}}
    export RUST_BACKTRACE=full
    export RUST_LOG=epkv_eval=debug,epkv_protocol=debug
    ./target/release/epkv-eval {{ARGS}}
