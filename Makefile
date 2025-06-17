.PHONY: up down restart logs test austin-web-scanner austin-web-scanner-realtime austin-tui-scanner austin-tui-scanner-realtime austin-help

run:
	docker-compose up -d

stop:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f redis

test:
	coverage run -m pytest -s -vv && coverage report -m

# Austin Web Commands
austin-web-scanner:
	@echo "Starting Austin Web for benchmark scanner..."
	@echo "Web interface will open in your browser"
	austin-web -S python -m fastscanner.benchmarks.benchmark_scanner

austin-web-scanner-realtime:
	@echo "Starting Austin Web for realtime benchmark scanner..."
	@echo "Web interface will open in your browser"
	austin-web -S python -m fastscanner.benchmarks.benchmark_scanner_realtime

# Austin TUI Commands
austin-tui-scanner:
	@echo "Starting Austin TUI for benchmark scanner..."
	austin-tui python -m fastscanner.benchmarks.benchmark_scanner

austin-tui-scanner-realtime:
	@echo "Starting Austin TUI for realtime benchmark scanner..."
	austin-tui python -m fastscanner.benchmarks.benchmark_scanner_realtime

austin-help:
	@echo "Austin Profiling Commands:"
	@echo ""
	@echo "Available Commands:"
	@echo "  make austin-web-scanner       - Austin Web for benchmark scanner"
	@echo "  make austin-web-scanner-realtime - Austin Web for realtime scanner"  
	@echo "  make austin-tui-scanner       - Austin TUI for benchmark scanner"
	@echo "  make austin-tui-scanner-realtime - Austin TUI for realtime scanner"
	@echo ""
	@echo "Direct Austin Commands (without Make):"
	@echo "  austin-web -S python -m fastscanner.benchmarks.benchmark_scanner"
	@echo "  austin-web -S python -m fastscanner.benchmarks.benchmark_scanner_realtime"
	@echo "  austin-tui python -m fastscanner.benchmarks.benchmark_scanner"
	@echo "  austin-tui python -m fastscanner.benchmarks.benchmark_scanner_realtime"
	@echo ""
