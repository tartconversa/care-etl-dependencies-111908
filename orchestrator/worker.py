"""外部演练程序的启动与回执校验。

进程退出、产物有效、可对外发布是三个不同事实：
- 退出码只记录，不单独决定成败（exit-after 模式退出 24 但回执已落盘）；
- 产物有效 = 回执存在、运行授权一致、记录的输入指纹与请求一致、
  产物字节摘要与回执一致；
- 是否可发布由发布组一致性另行判断，与本层无关。
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path


class ReceiptError(Exception):
    """回执缺失或不可验证。"""


def launch_worker(worker_path, request_path, out_dir, mode="ok", timeout=120):
    """同步启动演练程序，返回 (exit_code, stderr_tail)。"""
    proc = subprocess.run(
        [
            sys.executable,
            str(worker_path),
            "--request",
            str(request_path),
            "--directory",
            str(out_dir),
            "--mode",
            mode,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    tail = (proc.stderr or "").strip().splitlines()
    return proc.returncode, (tail[-1] if tail else "")


def validate_receipt(out_dir, run_token, request):
    """校验回执与产物；有效则返回回执 dict，否则抛 ReceiptError。"""
    out_dir = Path(out_dir)
    receipt_path = out_dir / "receipt.json"
    if not receipt_path.exists():
        raise ReceiptError("回执不存在")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReceiptError(f"回执不可读: {exc}") from exc
    if receipt.get("run_token") != run_token:
        raise ReceiptError("回执运行授权不符")
    if receipt.get("input_fingerprints") != request.get("input_fingerprints"):
        raise ReceiptError("回执记录的输入指纹与请求不符")
    artifact = Path(receipt.get("artifact") or out_dir / "artifact.json")
    if not artifact.exists():
        raise ReceiptError("产物不存在")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    if digest != receipt.get("sha256"):
        raise ReceiptError("产物摘要与回执不符")
    return receipt
