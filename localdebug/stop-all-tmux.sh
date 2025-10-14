#!/bin/bash

echo "=== StatsHouse Universal Stop Script ==="
echo "Stopping all StatsHouse tmux sessions and processes..."
echo ""

# Функция для остановки tmux сессии
stop_tmux_session() {
    local session_name="$1"
    
    if tmux has-session -t "$session_name" 2>/dev/null; then
        echo "Stopping tmux session: $session_name"
        
        # Получаем список окон
        local windows=$(tmux list-windows -t "$session_name" -F "#{window_index}" 2>/dev/null)
        
        if [ -n "$windows" ]; then
            echo "  Found windows: $windows"
            
            # Отправляем Ctrl+C во все окна
            for window in $windows; do
                echo "  Stopping window $window..."
                tmux send-keys -t "$session_name:$window" C-c
                sleep 1
            done
            
            # Ждем немного для graceful shutdown
            sleep 3
        fi
        
        # Убиваем сессию
        tmux kill-session -t "$session_name" 2>/dev/null
        echo "  ✓ Session $session_name stopped"
    else
        echo "  ✗ Session $session_name not found"
    fi
}

# Функция для остановки процессов по имени
stop_processes() {
    local process_name="$1"
    local description="$2"
    
    echo "Stopping $description processes..."
    
    # Ищем процессы
    local pids=$(pgrep -f "$process_name" 2>/dev/null)
    
    if [ -n "$pids" ]; then
        echo "  Found PIDs: $pids"
        
        # Сначала пытаемся graceful shutdown
        for pid in $pids; do
            echo "  Sending SIGTERM to PID $pid..."
            kill -TERM "$pid" 2>/dev/null
        done
        
        # Ждем
        sleep 3
        
        # Проверяем что процессы остановились
        local remaining_pids=$(pgrep -f "$process_name" 2>/dev/null)
        if [ -n "$remaining_pids" ]; then
            echo "  Some processes still running, sending SIGKILL..."
            for pid in $remaining_pids; do
                echo "  Sending SIGKILL to PID $pid..."
                kill -KILL "$pid" 2>/dev/null
            done
        fi
        
        echo "  ✓ $description processes stopped"
    else
        echo "  ✗ No $description processes found"
    fi
}

# Функция для остановки ClickHouse
#stop_clickhouse() {
#    echo "Stopping ClickHouse..."
#
#    if [ -f "./clickhouse-cluster/stop.sh" ]; then
#        ./clickhouse-cluster/stop.sh
#        echo "  ✓ ClickHouse cluster stopped"
#    else
#        echo "  ✗ ClickHouse stop script not found"
#    fi
#
#    # Дополнительно убиваем ClickHouse процессы
#    stop_processes "clickhouse" "ClickHouse"
#}

# Функция для очистки кешей
#cleanup_caches() {
#    echo "Cleaning up caches..."
#
#    local cache_dirs=(
#        "cache/agent"
#        "cache/agent-ingress-key1"
#        "cache/agent-ingress-key2"
#        "cache/agent-ingress2-key3"
#        "cache/aggregator"
#        "cache/metadata"
#        "cache/api"
#    )
#
#    for cache_dir in "${cache_dirs[@]}"; do
#        if [ -d "$cache_dir" ]; then
#            echo "  Cleaning $cache_dir..."
#            rm -rf "$cache_dir"/*
#        fi
#    done
#
#    echo "  ✓ Caches cleaned"
#}

# Основная логика
echo "Step 1: Stopping tmux sessions..."

# Список возможных сессий StatsHouse
local sessions=(
    "sh-local"
    "sh-local-2s"
    "sh-local-3s"
    "statshouse"
    "sh-test"
)

for session in "${sessions[@]}"; do
    stop_tmux_session "$session"
done

echo ""
echo "Step 2: Stopping StatsHouse processes..."

# Останавливаем процессы по имени
stop_processes "statshouse" "StatsHouse"
stop_processes "statshouse-igp" "Ingress Proxy"
stop_processes "statshouse-agg" "Aggregator"
stop_processes "statshouse-api" "API"
stop_processes "statshouse-metadata" "Metadata"

echo ""
echo "Step 3: Stopping ClickHouse..."
#stop_clickhouse

echo ""
echo "Step 4: Cleaning up caches..."
#cleanup_caches

echo ""
echo "Step 5: Final cleanup..."

# Убиваем все оставшиеся процессы StatsHouse
echo "Killing any remaining StatsHouse processes..."
pkill -f "statshouse" 2>/dev/null || true

# Очищаем временные файлы
echo "Cleaning temporary files..."
#rm -f localdebug/run-ingress-3shards.sh localdebug/run-ingress2-3shards.sh 2>/dev/null || true

echo ""
echo "=== CLEANUP COMPLETE ==="
echo "All StatsHouse services stopped and cleaned up."
echo ""

# Показываем статус
echo "Remaining StatsHouse processes:"
pgrep -f "statshouse" 2>/dev/null || echo "  None"

echo ""
echo "Remaining tmux sessions:"
tmux list-sessions 2>/dev/null | grep -E "(sh-|statshouse)" || echo "  None"

echo ""
echo "ClickHouse status:"
pgrep -f "clickhouse" 2>/dev/null || echo "  None"

echo ""
echo "All done! 🎉"
