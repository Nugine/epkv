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

generate-local-cluster which: build
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    export RUST_BACKTRACE=full
    ./target/release/epkv-eval cluster generate \
        --config crates/epkv-eval/tests/local-cluster-{{which}}.json \
        --target /tmp/epkv-cluster/config

local-server name: build
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    if [ -z "$EPKV_BENCHING" ]; then
        export RUST_BACKTRACE=full
        export RUST_LOG=epkv_server=debug,epkv_rocks=debug,epkv_epaxos=debug,epkv_protocol=debug
    fi
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

boot-local-cluster which: build
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    
    rm -rf /tmp/epkv-cluster
    just generate-local-cluster {{which}}

    mkdir -p target/local-cluster/log

    just local-monitor          >target/local-cluster/log/monitor.ansi    2>&1 &
    sleep 0.5s
    just local-server alpha     >target/local-cluster/log/alpha.ansi      2>&1 &
    just local-server beta      >target/local-cluster/log/beta.ansi       2>&1 &
    just local-server gamma     >target/local-cluster/log/gamma.ansi      2>&1 &
    if [ "{{which}}" == "5" ]; then
    just local-server delta     >target/local-cluster/log/delta.ansi      2>&1 &
    just local-server epsilon   >target/local-cluster/log/epsilon.ansi    2>&1 &
    fi
    sleep 1s
    ps -ef | rg 'epkv'

killall:
    killall epkv-server
    killall epkv-monitor

bench-local-case1 which key_size value_size cmd_count batch_size:
    #!/bin/bash -ex
    cd {{justfile_directory()}}
    
    mkdir -p target/local-cluster/bench
    TIME=`date -u +"%Y-%m-%d-%H-%M-%S"`
    CONFIG=crates/epkv-eval/tests/local-bench-{{which}}.json
    OUTPUT=target/local-cluster/bench/$TIME-case1.json

    ./target/release/epkv-eval bench \
        --config $CONFIG \
        --output $OUTPUT \
        case1 \
            --key-size {{key_size}}  \
            --value-size {{value_size}} \
            --cmd-count {{cmd_count}} \
            --batch-size {{batch_size}}

bench-local-case2 which key_size value_size cmd_count batch_size:
    #!/bin/bash -ex
    cd {{justfile_directory()}}

    mkdir -p target/local-cluster/bench
    TIME=`date -u +"%Y-%m-%d-%H-%M-%S"`
    CONFIG=crates/epkv-eval/tests/local-bench-{{which}}.json
    OUTPUT=target/local-cluster/bench/$TIME-case2.json

    ./target/release/epkv-eval bench \
        --config $CONFIG \
        --output $OUTPUT \
        case2 \
            --key-size {{key_size}}  \
            --value-size {{value_size}} \
            --cmd-count {{cmd_count}} \
            --batch-size {{batch_size}}

bench-local-case3 which value_size cmd_count conflict_rate interval_ms interval_count:
    #!/bin/bash -ex
    cd {{justfile_directory()}}

    mkdir -p target/local-cluster/bench
    TIME=`date -u +"%Y-%m-%d-%H-%M-%S"`
    CONFIG=crates/epkv-eval/tests/local-bench-{{which}}.json
    OUTPUT=target/local-cluster/bench/$TIME-case3.json

    ./target/release/epkv-eval bench \
        --config $CONFIG \
        --output $OUTPUT \
        case3 \
            --value-size {{value_size}} \
            --cmd-count {{cmd_count}} \
            --conflict-rate {{conflict_rate}} \
            --interval-ms {{interval_ms}} \
            --interval-count {{interval_count}}

bench-local-case4 which interval_ms="":
    #!/bin/bash -ex
    cd {{justfile_directory()}}

    mkdir -p target/local-cluster/bench
    TIME=`date -u +"%Y-%m-%d-%H-%M-%S"`
    CONFIG=crates/epkv-eval/tests/local-bench-{{which}}.json
    OUTPUT=target/local-cluster/bench/$TIME-case4.json
    LOG=target/local-cluster/log/$TIME-case4.log

    if [ -z "{{interval_ms}}" ]; then
        ./target/release/epkv-eval bench \
            --config $CONFIG \
            --output $OUTPUT \
            case4
    else
        ./target/release/epkv-eval bench \
            --config $CONFIG \
            --output $OUTPUT \
            case4 \
            --interval-ms {{interval_ms}} \
            > $LOG
    fi


bench-local-case5 which value_size conflict_rate interval cmds server_name:
    #!/bin/bash -ex
    cd {{justfile_directory()}}

    mkdir -p target/local-cluster/bench
    TIME=`date -u +"%Y-%m-%d-%H-%M-%S"`
    CONFIG=crates/epkv-eval/tests/local-bench-{{which}}.json
    OUTPUT=target/local-cluster/bench/$TIME-case5.json

    ./target/release/epkv-eval bench \
        --config $CONFIG \
        --output $OUTPUT \
        case5 \
        --value-size {{value_size}} \
        --conflict-rate {{conflict_rate}} \
        --cmds-per-interval {{cmds}} \
        --interval-ms {{interval}} \
        --server-name {{server_name}}
