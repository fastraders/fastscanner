import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional

from fastscanner.pkg.logging import load_logging_config

load_logging_config()
logger = logging.getLogger(__name__)


class AustinProfiler:
    def __init__(
        self,
        output_dir: str = "output/profiling",
        sampling_interval: int = 1000,
        mode: str = "wall",  # wall, cpu, or memory
        timeout: Optional[int] = None,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sampling_interval = sampling_interval
        self.mode = mode
        self.timeout = timeout

    def profile(self, script_path: str, output_name: Optional[str] = None) -> str:
        if output_name is None:
            timestamp = int(time.time())
            script_name = Path(script_path).stem
            output_name = f"{script_name}_{timestamp}.austin"

        output_path = self.output_dir / output_name

        cmd = ["austin", "-i", str(self.sampling_interval), "-o", str(output_path)]

        if self.mode == "memory":
            cmd.append("-m")
        elif self.mode == "cpu":
            cmd.append("-s")

        if self.timeout:
            cmd.extend(["-t", str(self.timeout)])

        cmd.extend(["poetry", "run", "python", "-m", script_path])

        logger.info(f"Profiling: {script_path}")
        logger.info(f"Command: {' '.join(cmd)}")

        try:
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            end_time = time.time()

            logger.info(f"Profiling completed in {end_time - start_time:.2f} seconds")
            logger.info(f"Profile saved to: {output_path}")
            if output_path.exists():
                size = output_path.stat().st_size / 1024
                logger.info(f"Profile file size: {size:.2f} KB")

            return str(output_path)

        except subprocess.CalledProcessError as e:
            logger.error(f"Profiling failed: {e}")
            logger.error(f"STDERR: {e.stderr}")
            raise

    def profile_multiple(self, scripts: List[str]) -> Dict[str, str]:
        results = {}

        for i, script in enumerate(scripts, 1):
            logger.info(f"Profiling script {i}/{len(scripts)}: {script}")

            try:
                profile_file = self.profile(script)
                results[script] = profile_file
            except Exception as e:
                logger.error(f"Failed to profile {script}: {e}")
                results[script] = None

        return results

    def get_profiling_summary(self, austin_file: str) -> Dict:
        if not os.path.exists(austin_file):
            raise FileNotFoundError(f"Austin file not found: {austin_file}")

        with open(austin_file, "r") as f:
            lines = [line for line in f if line.strip() and not line.startswith("#")]

        file_size = os.path.getsize(austin_file)

        return {
            "file_path": austin_file,
            "file_size_kb": round(file_size / 1024, 2),
            "total_samples": len(lines),
            "creation_time": time.ctime(os.path.getctime(austin_file)),
            "sampling_interval_us": self.sampling_interval,
            "profiling_mode": self.mode,
        }
