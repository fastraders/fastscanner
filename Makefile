.PHONY: up down restart logs test profile-help profile-benchmark profile-realtime profile-benchmarks profile-cmd-scripts profile-multiple profile-clean

run:
	docker-compose up -d

stop:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f redis

test:
	coverage run -m pytest -s -vv && coverage report -m


profile-help:
	@echo "FastScanner Profiling Commands:"
	@echo ""
	@echo "Basic Commands:"
	@echo "  make profile-benchmark        - Profile main benchmark scanner"
	@echo "  make profile-realtime         - Profile realtime scanner (30s)"
	@echo "  make profile-benchmarks       - Profile all benchmark scripts"
	@echo "  make profile-cmd-scripts      - Profile all cmd scripts"
	@echo ""
	@echo "Advanced Commands:"
	@echo "  make profile-realtime-long    - Profile realtime scanner (2 minutes)"
	@echo "  make profile-memory           - Profile with memory tracking"
	@echo "  make profile-cpu              - Profile with CPU tracking"
	@echo "  make profile-custom SCRIPTS='script1,script2' - Profile custom scripts"
	@echo ""
	@echo "Maintenance:"
	@echo "  make profile-clean            - Clean old profile files"
	@echo "  make profile-list             - List recent profile files"
	@echo ""

profile-benchmark:
	@echo "Profiling main benchmark scanner..."
	poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner

profile-realtime:
	@echo "Profiling realtime scanner for 30 seconds..."
	poetry run python -m fastscanner.profiler.cli realtime fastscanner.benchmarks.benchmark_scanner_realtime --duration 30

profile-realtime-long:
	@echo "Profiling realtime scanner for 2 minutes..."
	poetry run python -m fastscanner.profiler.cli realtime fastscanner.benchmarks.benchmark_scanner_realtime --duration 120

profile-benchmarks:
	@echo "Profiling all benchmark scripts..."
	poetry run python -m fastscanner.profiler.cli benchmarks

profile-cmd-scripts:
	@echo "Profiling all cmd scripts..."
	poetry run python -m fastscanner.profiler.cli cmd-scripts

profile-memory:
	@echo "Profiling benchmark with memory tracking..."
	poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner --mode memory

profile-cpu:
	@echo "Profiling benchmark with CPU tracking..."
	poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner --mode cpu

profile-high-res:
	@echo "Profiling benchmark with high resolution (100μs intervals)..."
	poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_scanner --interval 100

profile-custom:
	@echo "Profiling custom scripts: $(SCRIPTS)"
	poetry run python -m fastscanner.profiler.cli multiple "$(SCRIPTS)"

profile-polygon:
	@echo "Profiling Polygon realtime benchmark..."
	poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_polygon_realtime

profile-redis:
	@echo "Profiling Redis benchmark..."
	poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.benchmark_redis_read

profile-indicators:
	@echo "Profiling indicators calculation..."
	poetry run python -m fastscanner.profiler.cli script fastscanner.benchmarks.calculate_indicators

profile-clean:
	@echo "Cleaning old profile files..."
	@find output/profiling -name "*.austin" -mtime +7 -delete 2>/dev/null || echo "No old profiles to clean"
	@echo "Cleaned profile files older than 7 days"

profile-list:
	@echo "Recent profile files:"
	@ls -la output/profiling/*.austin 2>/dev/null | tail -10 || echo "No profile files found"

profile-analyze-latest:
	@echo "Analyzing latest profile file..."
	@LATEST=$$(ls -t output/profiling/*.austin 2>/dev/null | head -1); \
	if [ -n "$$LATEST" ]; then \
		echo "Latest profile: $$LATEST"; \
		ls -lh "$$LATEST"; \
	else \
		echo "No profile files found"; \
	fi