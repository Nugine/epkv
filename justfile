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
    cargo build --release --offline

miri:
    MIRIFLAGS=-Zmiri-backtrace=full \
    cargo miri test -p epkv-utils -- --test-threads=1 \
        --skip watermark \
        --skip flag_group \
        --skip stepper

test:
    cargo test --release --offline

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

boot-local-cluster: build
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    rm -rf /tmp/epkv-cluster
    just generate-local-cluster
    just local-monitor >target/monitor.ansi 2>&1 &
    sleep 0.5s
    just local-server alpha     >target/alpha.ansi      2>&1 &
    just local-server beta      >target/beta.ansi       2>&1 &
    just local-server gamma     >target/gamma.ansi      2>&1 &
    just local-server delta     >target/delta.ansi      2>&1 &
    just local-server epsilon   >target/epsilon.ansi    2>&1 &
    sleep 1s
    ps -ef | rg 'epkv'

killall:
    killall epkv-server
    killall epkv-monitor

bench-local-case1 key_size value_size cmd_count batch_size:
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    TIME=`date -u +"%Y-%m-%d-%H-%M-%S"`
    ./target/release/epkv-eval bench \
        --config crates/epkv-eval/tests/local-bench.json \
        --target target/$TIME \
        case1 \
            --key-size {{key_size}}  \
            --value-size {{value_size}} \
            --cmd-count {{cmd_count}} \
            --batch-size {{batch_size}}

bench-local-case2 key_size value_size cmd_count batch_size:
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    TIME=`date -u +"%Y-%m-%d-%H-%M-%S"`
    ./target/release/epkv-eval bench \
        --config crates/epkv-eval/tests/local-bench.json \
        --target target/$TIME \
        case2 \
            --key-size {{key_size}}  \
            --value-size {{value_size}} \
            --cmd-count {{cmd_count}} \
            --batch-size {{batch_size}}
