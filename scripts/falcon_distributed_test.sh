#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# FalconFS Single-Node Distributed Test Deployment Script
# Architecture: 1 CN (Coordinator) + 2 DNs (Workers) + 2 Clients (FUSE)
# Purpose: Local development and testing for distributed scenarios
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_DIR="${FALCONFS_INSTALL_DIR:-/usr/local/falconfs}"
DEPLOY_DIR="$INSTALL_DIR/deploy"

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# ============================================================================
# Configuration
# ============================================================================

# Meta nodes (PostgreSQL)
CN_PORT=55500
CN_POOLER_PORT=55510
DN1_PORT=55520
DN1_POOLER_PORT=55530
DN2_PORT=55540
DN2_POOLER_PORT=55550
META_DB_DIR="$HOME/falcon_metadata"

# Client nodes (FUSE mounts)
CLIENT1_RPC_PORT=56039
CLIENT1_MNT="/tmp/falcon_mnt1"
CLIENT1_CACHE="/tmp/falcon_client1_cache"
CLIENT1_LOG="/tmp/falcon_client1.log"

CLIENT2_RPC_PORT=56042
CLIENT2_MNT="/tmp/falcon_mnt2"
CLIENT2_CACHE="/tmp/falcon_client2_cache"
CLIENT2_LOG="/tmp/falcon_client2.log"

# ============================================================================
# Helper Functions
# ============================================================================

cleanup() {
    log_step "Cleaning up old environment..."
    
    # Stop all falcon_client processes
    pkill -9 falcon_client 2>/dev/null || true
    
    # Unmount all FUSE mounts
    for mnt in "$CLIENT1_MNT" "$CLIENT2_MNT"; do
        if mount | grep -q "$mnt"; then
            sudo umount -l "$mnt" 2>/dev/null || true
        fi
    done
    
    # Stop Meta (PostgreSQL)
    stop_pg_node "CN" "$META_DB_DIR/coordinator0" || true
    stop_pg_node "DN1" "$META_DB_DIR/worker0" || true
    stop_pg_node "DN2" "$META_DB_DIR/worker1" || true
    kill_listener_on_port "$CN_PORT" "CN SQL" || true
    kill_listener_on_port "$CN_POOLER_PORT" "CN pooler" || true
    kill_listener_on_port "$DN1_PORT" "DN1 SQL" || true
    kill_listener_on_port "$DN1_POOLER_PORT" "DN1 pooler" || true
    kill_listener_on_port "$DN2_PORT" "DN2 SQL" || true
    kill_listener_on_port "$DN2_POOLER_PORT" "DN2 pooler" || true
    
    # Clean temporary files
    rm -rf "$META_DB_DIR" 2>/dev/null || true
    rm -rf "$CLIENT1_CACHE" "$CLIENT2_CACHE" 2>/dev/null || true
    rm -f "$CLIENT1_LOG" "$CLIENT2_LOG" 2>/dev/null || true
    
    sleep 2
    log_info "Cleanup completed"
}

check_port() {
    local port=$1
    if ss -tuln 2>/dev/null | grep -q ":${port} " || netstat -tuln 2>/dev/null | grep -q ":${port} "; then
        log_error "Port $port is already in use"
        return 1
    fi
    return 0
}

stop_pg_node() {
    local node_name=$1
    local data_dir=$2

    if [ ! -d "$data_dir" ]; then
        return 0
    fi

    if ! pg_ctl status -D "$data_dir" >/dev/null 2>&1; then
        log_info "$node_name already stopped"
        return 0
    fi

    if pg_ctl stop -D "$data_dir" -m fast -w -t 30 >/dev/null 2>&1; then
        log_info "Stopped $node_name"
        return 0
    fi

    log_warn "$node_name did not stop cleanly; forcing shutdown"

    local pid_file="$data_dir/postmaster.pid"
    local pid=""
    if [ -f "$pid_file" ]; then
        pid="$(sed -n '1p' "$pid_file" 2>/dev/null || true)"
    fi

    if [ -n "$pid" ]; then
        kill -TERM "$pid" 2>/dev/null || true
        sleep 2
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi

    for _ in {1..10}; do
        if ! pg_ctl status -D "$data_dir" >/dev/null 2>&1; then
            log_info "Stopped $node_name (forced)"
            return 0
        fi
        sleep 1
    done

    log_error "Failed to stop $node_name"
    return 1
}

kill_listener_on_port() {
    local port=$1
    local label=$2
    local pids=""

    pids="$(ss -lptn "( sport = :${port} )" 2>/dev/null | awk -F'pid=' 'NF>1{split($2,a,","); print a[1]}' | sort -u)"
    if [ -z "$pids" ]; then
        return 0
    fi

    log_warn "Port $port still occupied ($label); terminating listener(s)"
    for pid in $pids; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 1

    pids="$(ss -lptn "( sport = :${port} )" 2>/dev/null | awk -F'pid=' 'NF>1{split($2,a,","); print a[1]}' | sort -u)"
    for pid in $pids; do
        kill -KILL "$pid" 2>/dev/null || true
    done
}

# ============================================================================
# Build and Install
# ============================================================================

build_and_install() {
    log_step "========================================="
    log_step "Building and Installing FalconFS"
    log_step "========================================="
    
    cd "$PROJECT_DIR"
    
    # Build
    log_step "Building FalconFS..."
    ./build.sh build falcon
    
    # Install
    log_step "Installing FalconFS..."
    sudo ./build.sh install
    
    log_info "Build and install completed"
}

# ============================================================================
# Start Services
# ============================================================================

start_all() {
    log_step "========================================="
    log_step "Starting FalconFS Single-Node Cluster"
    log_step "Architecture: 1 CN + 2 DNs + 2 Clients"
    log_step "========================================="
    
    # 1. Clean old environment
    cleanup
    
    # 2. Check port availability
    log_step "Checking port availability..."
    check_port $CN_PORT || exit 1
    check_port $CN_POOLER_PORT || exit 1
    check_port $DN1_PORT || exit 1
    check_port $DN1_POOLER_PORT || exit 1
    check_port $DN2_PORT || exit 1
    check_port $DN2_POOLER_PORT || exit 1
    check_port $CLIENT1_RPC_PORT || exit 1
    check_port $CLIENT2_RPC_PORT || exit 1
    log_info "All ports are available"
    
    # 3. Set environment variables
    export FALCONFS_INSTALL_DIR="$INSTALL_DIR"
    export PATH="$INSTALL_DIR/falcon_client/bin:$(pg_config --bindir):${PATH:-}"
    export LD_LIBRARY_PATH="$INSTALL_DIR/falcon_client/lib:$INSTALL_DIR/falcon_meta/lib:$(pg_config --libdir):${LD_LIBRARY_PATH:-}"
    export CONFIG_FILE="$INSTALL_DIR/falcon_client/config/config.json"
    
    # 4. Install Falcon extension to PostgreSQL
    log_step "【1/6】Installing Falcon extension to PostgreSQL..."
    local pg_ext_dir="$(pg_config --sharedir)/extension"
    local pg_lib_dir="$(pg_config --pkglibdir)"
    sudo mkdir -p "$pg_ext_dir" "$pg_lib_dir"
    sudo cp -f "$INSTALL_DIR/falcon_meta/share/extension"/falcon* "$pg_ext_dir/" 2>/dev/null || true
    sudo cp -f "$INSTALL_DIR/falcon_meta/lib/postgresql"/falcon*.so "$pg_lib_dir/" 2>/dev/null || true
    sudo cp -f "$INSTALL_DIR/falcon_meta/lib/postgresql"/libbrpcplugin.so "$pg_lib_dir/" 2>/dev/null || true
    log_info "Falcon extension installed"
    
    # 5. Initialize and start CN (Coordinator - server_id=0)
    log_step "【2/6】Starting CN Coordinator (port: $CN_PORT)..."
    local cn_path="$META_DB_DIR/coordinator0"
    mkdir -p "$cn_path"
    
    if [ ! -d "$cn_path/PG_VERSION" ]; then
        initdb -D "$cn_path" --username=$USER
    fi
    
    local comm_plugin_path="$INSTALL_DIR/falcon_meta/lib/postgresql/libbrpcplugin.so"
    cat >"$cn_path/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$CN_PORT
listen_addresses = '*'
wal_level = logical
max_prepared_transactions = 100
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $CN_POOLER_PORT
falcon_connection_pool.pool_size = 32
falcon_connection_pool.shmem_size = 256
falcon_connection_pool.batch_size = 1024
falcon_connection_pool.wait_adjust = 1
falcon_connection_pool.wait_min = 1
falcon_connection_pool.wait_max = 500
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '127.0.0.1'
falcon_plugin.directory = '$INSTALL_DIR/plugins'
falcon.local_ip = '127.0.0.1'
falcon.perf_enabled = on
EOF
    echo "host all all 0.0.0.0/0 trust" >>"$cn_path/pg_hba.conf"
    
    pg_ctl start -l "/tmp/falcon_cn.log" -D "$cn_path" -c
    sleep 3
    
    # Verify CN is running
    if ! pg_ctl status -D "$cn_path" >/dev/null 2>&1; then
        log_error "CN failed to start. Check log: /tmp/falcon_cn.log"
        tail -30 /tmp/falcon_cn.log
        exit 1
    fi
    
    psql -d postgres -h 127.0.0.1 -p $CN_PORT -c "CREATE EXTENSION IF NOT EXISTS falcon;"
    log_info "CN started successfully"
    
    # 6. Initialize and start DN1 (Worker - server_id=1)
    log_step "【3/8】Starting DN1 Worker (port: $DN1_PORT)..."
    local dn1_path="$META_DB_DIR/worker0"
    mkdir -p "$dn1_path"
    
    if [ ! -d "$dn1_path/PG_VERSION" ]; then
        initdb -D "$dn1_path" --username=$USER
    fi
    
    cat >"$dn1_path/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$DN1_PORT
listen_addresses = '*'
wal_level = logical
max_prepared_transactions = 100
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $DN1_POOLER_PORT
falcon_connection_pool.pool_size = 32
falcon_connection_pool.shmem_size = 256
falcon_connection_pool.batch_size = 1024
falcon_connection_pool.wait_adjust = 1
falcon_connection_pool.wait_min = 1
falcon_connection_pool.wait_max = 500
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '127.0.0.1'
falcon_plugin.directory = '$INSTALL_DIR/plugins'
falcon.local_ip = '127.0.0.1'
falcon.perf_enabled = on
EOF
    echo "host all all 0.0.0.0/0 trust" >>"$dn1_path/pg_hba.conf"
    
    pg_ctl start -l "/tmp/falcon_dn1.log" -D "$dn1_path" -c
    sleep 3
    
    # Verify DN1 is running
    if ! pg_ctl status -D "$dn1_path" >/dev/null 2>&1; then
        log_error "DN1 failed to start. Check log: /tmp/falcon_dn1.log"
        tail -30 /tmp/falcon_dn1.log
        exit 1
    fi
    
    psql -d postgres -h 127.0.0.1 -p $DN1_PORT -c "CREATE EXTENSION IF NOT EXISTS falcon;"
    log_info "DN1 started successfully"
    
    # 7. Initialize and start DN2 (Worker - server_id=2)
    log_step "【4/8】Starting DN2 Worker (port: $DN2_PORT)..."
    local dn2_path="$META_DB_DIR/worker1"
    mkdir -p "$dn2_path"
    
    if [ ! -d "$dn2_path/PG_VERSION" ]; then
        initdb -D "$dn2_path" --username=$USER
    fi
    
    cat >"$dn2_path/postgresql.conf" <<EOF
shared_preload_libraries = 'falcon'
port=$DN2_PORT
listen_addresses = '*'
wal_level = logical
max_prepared_transactions = 100
max_replication_slots = 8
max_wal_senders = 8
falcon_connection_pool.port = $DN2_POOLER_PORT
falcon_connection_pool.pool_size = 32
falcon_connection_pool.shmem_size = 256
falcon_connection_pool.batch_size = 1024
falcon_connection_pool.wait_adjust = 1
falcon_connection_pool.wait_min = 1
falcon_connection_pool.wait_max = 500
falcon_communication.plugin_path = '$comm_plugin_path'
falcon_communication.server_ip = '127.0.0.1'
falcon_plugin.directory = '$INSTALL_DIR/plugins'
falcon.local_ip = '127.0.0.1'
falcon.perf_enabled = on
EOF
    echo "host all all 0.0.0.0/0 trust" >>"$dn2_path/pg_hba.conf"
    
    pg_ctl start -l "/tmp/falcon_dn2.log" -D "$dn2_path" -c
    sleep 3
    
    # Verify DN2 is running
    if ! pg_ctl status -D "$dn2_path" >/dev/null 2>&1; then
        log_error "DN2 failed to start. Check log: /tmp/falcon_dn2.log"
        tail -30 /tmp/falcon_dn2.log
        exit 1
    fi
    
    psql -d postgres -h 127.0.0.1 -p $DN2_PORT -c "CREATE EXTENSION IF NOT EXISTS falcon;"
    log_info "DN2 started successfully"
    
    # 8. Register servers in the cluster
    # IMPORTANT: Register on ALL nodes so they all know each other
    log_step "【5/8】Registering servers to cluster..."

    local node_ports=("$CN_PORT" "$DN1_PORT" "$DN2_PORT")

    for target_port in "${node_ports[@]}"; do
        local cn_local=false
        local dn1_local=false
        local dn2_local=false

        if [ "$target_port" -eq "$CN_PORT" ]; then
            cn_local=true
        elif [ "$target_port" -eq "$DN1_PORT" ]; then
            dn1_local=true
        elif [ "$target_port" -eq "$DN2_PORT" ]; then
            dn2_local=true
        fi

        psql -d postgres -h 127.0.0.1 -p "$target_port" \
            -c "SELECT falcon_insert_foreign_server(0, 'cn0', '127.0.0.1', $CN_PORT, $cn_local, '$USER');"
        psql -d postgres -h 127.0.0.1 -p "$target_port" \
            -c "SELECT falcon_insert_foreign_server(1, 'worker0', '127.0.0.1', $DN1_PORT, $dn1_local, '$USER');"
        psql -d postgres -h 127.0.0.1 -p "$target_port" \
            -c "SELECT falcon_insert_foreign_server(2, 'worker1', '127.0.0.1', $DN2_PORT, $dn2_local, '$USER');"
    done
    
    log_info "Servers registered"
    
    # 9. Build shard table and initialize services
    log_step "【6/8】Building shard table and initializing services..."
    
    # Build shard table on all nodes (distributes shards across workers)
    psql -d postgres -h 127.0.0.1 -p $CN_PORT -c "SELECT falcon_build_shard_table(50);"
    psql -d postgres -h 127.0.0.1 -p $DN1_PORT -c "SELECT falcon_build_shard_table(50);"
    psql -d postgres -h 127.0.0.1 -p $DN2_PORT -c "SELECT falcon_build_shard_table(50);"
    
    # Create tables and start background services on all nodes
    for port in $CN_PORT $DN1_PORT $DN2_PORT; do
        psql -d postgres -h 127.0.0.1 -p $port <<EOF
SELECT falcon_create_distributed_data_table();
SELECT falcon_create_slice_table();
SELECT falcon_create_kvmeta_table();
SELECT falcon_start_background_service();
EOF
    done
    
    # Create root directory on CN only
    psql -d postgres -h 127.0.0.1 -p $CN_PORT -c "SELECT falcon_plain_mkdir('/');"
    
    log_info "Cluster initialization completed"
    sleep 2
    
    # 10. Start Client1 and Client2 with full cluster view
    # Note: Both clients will attempt to connect to each other in cluster_view
    # Connection failures are acceptable during initial startup
    log_step "【7/8】Starting Client1 (mount: $CLIENT1_MNT, RPC: $CLIENT1_RPC_PORT)..."
    mkdir -p "$CLIENT1_MNT" "$CLIENT1_CACHE"
    for i in {0..100}; do mkdir -p "$CLIENT1_CACHE/$i" 2>/dev/null || true; done
    
    # Create client-specific config with full cluster view
    cat > /tmp/falcon_client1_config.json << EOF
{
    "main": {
        "falcon_log_dir": "/tmp",
        "falcon_log_level": "INFO",
        "falcon_log_max_size_mb": 10,
        "falcon_cache_root": "$CLIENT1_CACHE",
        "falcon_dir_num": 101,
        "falcon_block_size": 524288,
        "falcon_read_big_file_size": 2097152,
        "falcon_preblock_num": 1000,
        "falcon_max_open_num": 0,
        "falcon_node_id": 0,
        "falcon_cluster_view": ["127.0.0.1:$CLIENT1_RPC_PORT", "127.0.0.1:$CLIENT2_RPC_PORT"],
        "falcon_thread_num": 50,
        "falcon_server_ip": "127.0.0.1",
        "falcon_server_port": "$CN_POOLER_PORT",
        "falcon_async": false,
        "falcon_persist": false,
        "falcon_eviction": 0.1,
        "falcon_is_inference": false,
        "falcon_mount_path": "$CLIENT1_MNT",
        "falcon_to_local": true,
        "falcon_log_reserved_num": 3,
        "falcon_log_reserved_time": 1,
        "falcon_stat_max": true,
        "falcon_use_prometheus": false,
        "falcon_prometheus_port": "50040"
    }
}
EOF
    
    nohup env CONFIG_FILE=/tmp/falcon_client1_config.json falcon_client "$CLIENT1_MNT" -f -o direct_io -o attr_timeout=200 -o entry_timeout=200 \
        -brpc true -rpc_endpoint="0.0.0.0:${CLIENT1_RPC_PORT}" \
        -socket_max_unwritten_bytes=268435456 > "$CLIENT1_LOG" 2>&1 &
    CLIENT1_PID=$!
    sleep 5
    
    # 11. Start Client2  
    log_step "【8/8】Starting Client2 (mount: $CLIENT2_MNT, RPC: $CLIENT2_RPC_PORT)..."
    mkdir -p "$CLIENT2_MNT" "$CLIENT2_CACHE"
    for i in {0..100}; do mkdir -p "$CLIENT2_CACHE/$i" 2>/dev/null || true; done
    
    # Create client-specific config with full cluster view
    cat > /tmp/falcon_client2_config.json << EOF
{
    "main": {
        "falcon_log_dir": "/tmp",
        "falcon_log_level": "INFO",
        "falcon_log_max_size_mb": 10,
        "falcon_cache_root": "$CLIENT2_CACHE",
        "falcon_dir_num": 101,
        "falcon_block_size": 524288,
        "falcon_read_big_file_size": 2097152,
        "falcon_preblock_num": 1000,
        "falcon_max_open_num": 0,
        "falcon_node_id": 1,
        "falcon_cluster_view": ["127.0.0.1:$CLIENT1_RPC_PORT", "127.0.0.1:$CLIENT2_RPC_PORT"],
        "falcon_thread_num": 50,
        "falcon_server_ip": "127.0.0.1",
        "falcon_server_port": "$CN_POOLER_PORT",
        "falcon_async": false,
        "falcon_persist": false,
        "falcon_eviction": 0.1,
        "falcon_is_inference": false,
        "falcon_mount_path": "$CLIENT2_MNT",
        "falcon_to_local": true,
        "falcon_log_reserved_num": 3,
        "falcon_log_reserved_time": 1,
        "falcon_stat_max": true,
        "falcon_use_prometheus": false,
        "falcon_prometheus_port": "50041"
    }
}
EOF
    
    # Wait before starting Client2
    sleep 2
    
    nohup env CONFIG_FILE=/tmp/falcon_client2_config.json falcon_client "$CLIENT2_MNT" -f -o direct_io -o attr_timeout=200 -o entry_timeout=200 \
        -brpc true -rpc_endpoint="0.0.0.0:${CLIENT2_RPC_PORT}" \
        -socket_max_unwritten_bytes=268435456 > "$CLIENT2_LOG" 2>&1 &
    CLIENT2_PID=$!
    sleep 8
    
    # 12. Verify cluster status
    echo ""
    log_step "========================================="
    log_info "✅ FalconFS cluster started successfully!"
    log_step "========================================="
    echo ""
    log_info "Cluster Topology:"
    log_info "  CN (Coordinator): 127.0.0.1:$CN_PORT (Pooler: $CN_POOLER_PORT)"
    log_info "  DN1 (Worker):     127.0.0.1:$DN1_PORT (Pooler: $DN1_POOLER_PORT)"
    log_info "  DN2 (Worker):     127.0.0.1:$DN2_PORT (Pooler: $DN2_POOLER_PORT)"
    log_info "  Client1:          Mount: $CLIENT1_MNT (RPC: $CLIENT1_RPC_PORT)"
    log_info "  Client2:          Mount: $CLIENT2_MNT (RPC: $CLIENT2_RPC_PORT)"
    echo ""
    log_info "Test Commands:"
    log_info "  # Test Client1"
    log_info "  echo 'test from client1' > $CLIENT1_MNT/test1.txt"
    log_info "  cat $CLIENT1_MNT/test1.txt"
    echo ""
    log_info "  # Test Client2"
    log_info "  echo 'test from client2' > $CLIENT2_MNT/test2.txt"
    log_info "  cat $CLIENT2_MNT/test2.txt"
    echo ""
    log_info "  # Cross-client access (verify distributed consistency)"
    log_info "  ls -la $CLIENT1_MNT/"
    log_info "  ls -la $CLIENT2_MNT/"
    echo ""
    log_info "Stop Command:"
    log_info "  $0 stop"
}

# ============================================================================
# Stop Services
# ============================================================================

stop_all() {
    log_step "Stopping FalconFS cluster..."
    
    # Stop Clients (unmount FUSE)
    for mnt in "$CLIENT1_MNT" "$CLIENT2_MNT"; do
        if mount | grep -q "$mnt"; then
            sudo umount -l "$mnt" 2>/dev/null || true
            log_info "Unmounted $mnt"
        fi
    done
    
    pkill -9 falcon_client 2>/dev/null || true
    
    # Stop Meta (PostgreSQL)
    stop_pg_node "CN" "$META_DB_DIR/coordinator0" || true
    stop_pg_node "DN1" "$META_DB_DIR/worker0" || true
    stop_pg_node "DN2" "$META_DB_DIR/worker1" || true
    kill_listener_on_port "$CN_PORT" "CN SQL" || true
    kill_listener_on_port "$CN_POOLER_PORT" "CN pooler" || true
    kill_listener_on_port "$DN1_PORT" "DN1 SQL" || true
    kill_listener_on_port "$DN1_POOLER_PORT" "DN1 pooler" || true
    kill_listener_on_port "$DN2_PORT" "DN2 SQL" || true
    kill_listener_on_port "$DN2_POOLER_PORT" "DN2 pooler" || true
    
    sleep 2
    log_info "All services stopped"
}

# ============================================================================
# Show Status
# ============================================================================

show_status() {
    echo "========================================="
    echo " FalconFS Cluster Status"
    echo "========================================="
    echo ""
    
    # Meta status
    echo "【Meta Services】"
    if pg_ctl status -D "$META_DB_DIR/coordinator0" >/dev/null 2>&1; then
        log_info "CN (Coordinator) running (port: $CN_PORT)"
    else
        log_error "CN not running"
    fi
    
    if pg_ctl status -D "$META_DB_DIR/worker0" >/dev/null 2>&1; then
        log_info "DN1 (Worker) running (port: $DN1_PORT)"
    else
        log_error "DN1 not running"
    fi

    if pg_ctl status -D "$META_DB_DIR/worker1" >/dev/null 2>&1; then
        log_info "DN2 (Worker) running (port: $DN2_PORT)"
    else
        log_error "DN2 not running"
    fi
    echo ""
    
    # Client status
    echo "【Client Nodes】"
    if mount | grep -q "$CLIENT1_MNT"; then
        log_info "Client1 mounted: $CLIENT1_MNT"
    else
        log_error "Client1 not mounted"
    fi
    
    if mount | grep -q "$CLIENT2_MNT"; then
        log_info "Client2 mounted: $CLIENT2_MNT"
    else
        log_error "Client2 not mounted"
    fi
    echo ""
    
    # Port listening
    echo "【Port Listening】"
    ss -tuln 2>/dev/null | grep -E "(${CN_PORT}|${DN1_PORT}|${DN2_PORT}|${CLIENT1_RPC_PORT}|${CLIENT2_RPC_PORT})" || \
        log_warn "Cannot check port status"
}

# ============================================================================
# Run Tests
# ============================================================================

run_test() {
    log_step "Running distributed tests..."
    
    # Check if mounts are ready
    if ! mount | grep -q "$CLIENT1_MNT" || ! mount | grep -q "$CLIENT2_MNT"; then
        log_error "Clients not mounted. Please run '$0 start' first"
        exit 1
    fi
    
    # Test 1: Client1 write
    log_step "【Test 1】Client1 writing file..."
    echo "Write time: $(date)" > "$CLIENT1_MNT/test_client1.txt"
    echo "Test content: Hello from Client1" >> "$CLIENT1_MNT/test_client1.txt"
    log_info "Client1 write successful"
    
    # Test 2: Client2 write
    log_step "【Test 2】Client2 writing file..."
    echo "Write time: $(date)" > "$CLIENT2_MNT/test_client2.txt"
    echo "Test content: Hello from Client2" >> "$CLIENT2_MNT/test_client2.txt"
    log_info "Client2 write successful"
    
    # Test 3: Client1 read its own file
    log_step "【Test 3】Client1 reading own file..."
    cat "$CLIENT1_MNT/test_client1.txt"
    log_info "Client1 read successful"
    
    # Test 4: Client2 read its own file
    log_step "【Test 4】Client2 reading own file..."
    cat "$CLIENT2_MNT/test_client2.txt"
    log_info "Client2 read successful"
    
    # Test 5: Cross-client visibility
    log_step "【Test 5】Checking cross-client file visibility..."
    echo "Client1 mount content:"
    ls -la "$CLIENT1_MNT/" | head -10
    echo ""
    echo "Client2 mount content:"
    ls -la "$CLIENT2_MNT/" | head -10
    
    # Test 6: Large file write
    log_step "【Test 6】Large file write test (10MB)..."
    dd if=/dev/urandom of="$CLIENT1_MNT/large_file.bin" bs=1M count=10 2>&1 | tail -1
    ls -lh "$CLIENT1_MNT/large_file.bin"
    log_info "Large file write successful"
    
    # Test 7: Concurrent write
    log_step "【Test 7】Concurrent write test..."
    for i in {1..5}; do
        echo "Concurrent test $i from Client1" > "$CLIENT1_MNT/concurrent_$i.txt" &
        echo "Concurrent test $i from Client2" > "$CLIENT2_MNT/concurrent_$i.txt" &
    done
    wait
    log_info "Concurrent write completed"
    
    # Cleanup test files
    log_step "Cleaning up test files..."
    rm -f "$CLIENT1_MNT"/test_*.txt "$CLIENT2_MNT"/test_*.txt
    rm -f "$CLIENT1_MNT"/large_file.bin "$CLIENT1_MNT"/concurrent_*.txt
    rm -f "$CLIENT2_MNT"/concurrent_*.txt
    
    echo ""
    log_step "========================================="
    log_info "✅ All tests passed!"
    log_step "========================================="
}

run_kv_test() {
    log_step "Running FalconFS KV cache standalone tests..."

    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:${PYTHONPATH:-}"

    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_reference_unit.py"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_metadata_reference.py"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_store_reference.py"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_brpc_contract_reference.py"

    log_info "KV standalone tests passed"
}

run_kv_fault_test() {
    log_step "Running FalconFS KV cache fault tests..."

    export PYTHONPATH="$PROJECT_DIR/vllm_kv_cache/python:${PYTHONPATH:-}"
    python3 "$PROJECT_DIR/vllm_kv_cache/test/test_kv_fault_reference.py"

    log_info "KV fault tests passed"
}

# ============================================================================
# KV Metadata End-to-End Stress Test
# ============================================================================
# Drives the FalconFS BRPC metadata service against the running cluster with
# varying concurrent clients and batch sizes to validate system correctness
# (allocation uniqueness, CAS version correctness, lookup/renew/free flow,
# concurrent dedup, and bitmap+catalog cleanup between sweeps).

run_kv_meta_stress_test() {
    log_step "Running FalconFS KV metadata end-to-end stress tests..."

    local stress_bin="$PROJECT_DIR/build/tests/falcon_kv/FalconKVMetadataStressE2E"
    if [ ! -x "$stress_bin" ]; then
        log_step "Stress binary missing; building it now..."
        (cd "$PROJECT_DIR/build" && ninja FalconKVMetadataStressE2E)
    fi

    local endpoints=("127.0.0.1:$DN1_POOLER_PORT" "127.0.0.1:$DN2_POOLER_PORT" "127.0.0.1:$CN_POOLER_PORT")
    local rc=0
    for ep in "${endpoints[@]}"; do
        log_step "  KV stress sweep on $ep"
        if ! "$stress_bin" --endpoint "$ep" --iterations 1 --dedup-clients 8; then
            log_step "  KV stress sweep FAILED on $ep"
            rc=1
        fi
    done

    # Verify catalog cleanup on every DN: every kvblock_table row created by
    # the sweep must have been DELETEd by the test's force-free phase. The
    # stress test uses block hashes prefixed with "stress_" so we only count
    # those.
    local cleanup_rc=0
    for port in "$DN1_PORT" "$DN2_PORT"; do
        local rows
        rows=$(psql -d postgres -h 127.0.0.1 -p "$port" -tAXc \
            "SELECT count(*) FROM pg_catalog.falcon_kvblock_table WHERE block_hash LIKE 'stress_%';" 2>/dev/null \
            || echo "ERR")
        if [ "$rows" != "0" ]; then
            log_step "  Catalog cleanup FAILED on DN port=$port (residual rows=$rows)"
            cleanup_rc=1
        else
            log_info "  Catalog clean on DN port=$port"
        fi
    done

    if [ $rc -ne 0 ] || [ $cleanup_rc -ne 0 ]; then
        log_step "KV metadata stress tests FAILED"
        return 1
    fi
    log_info "KV metadata stress tests passed"
}

# ============================================================================
# Main Entry
# ============================================================================

usage() {
    echo "Usage: $0 {build|start|stop|restart|status|test|kv-test|kv-fault-test|kv-meta-stress-test}"
    echo ""
    echo "Commands:"
    echo "  build   - Build and install FalconFS"
    echo "  start   - Start single-node cluster (1 CN + 2 DNs + 2 Clients)"
    echo "  stop    - Stop all services"
    echo "  restart - Restart all services"
    echo "  status  - Show service status"
    echo "  test    - Run distributed tests"
    echo "  kv-test - Run KV cache standalone tests (no vLLM required)"
    echo "  kv-fault-test - Run KV cache fault tests (no vLLM required)"
    echo "  kv-meta-stress-test - Run KV metadata end-to-end stress tests against the running cluster"
    exit 1
}

case "${1:-}" in
    build)
        build_and_install
        ;;
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    restart)
        stop_all
        sleep 2
        start_all
        ;;
    status)
        show_status
        ;;
    test)
        run_test
        ;;
    kv-test)
        run_kv_test
        ;;
    kv-meta-stress-test)
        run_kv_meta_stress_test
        ;;
    kv-fault-test)
        run_kv_fault_test
        ;;
    *)
        usage
        ;;
esac
