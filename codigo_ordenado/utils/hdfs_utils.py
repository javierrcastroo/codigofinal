from __future__ import annotations

import subprocess
from typing import List, Tuple


def _run(cmd: List[str]) -> Tuple[int, str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = p.communicate()
    return p.returncode, (out or "").strip(), (err or "").strip()


def _hdfs(args: List[str]) -> Tuple[int, str, str]:
    return _run(["hdfs", "dfs"] + args)


def _hdfs_must(args: List[str]) -> str:
    code, out, err = _hdfs(args)
    if code != 0:
        raise RuntimeError(
            "HDFS command failed: hdfs dfs {}\n{}\n{}".format(" ".join(args), out, err)
        )
    return out


def hdfs_exists(path: str) -> bool:
    code, _, _ = _hdfs(["-test", "-e", path])
    return code == 0


def hdfs_mkdir_p(path: str) -> None:
    _hdfs_must(["-mkdir", "-p", path])


def hdfs_rm(path: str, recursive: bool = False) -> None:
    args = ["-rm", "-skipTrash"]
    if recursive:
        args.append("-r")
    args.append(path)
    _hdfs_must(args)


def hdfs_rm_if_exists(path: str, recursive: bool = False) -> None:
    if hdfs_exists(path):
        hdfs_rm(path, recursive=recursive)


def hdfs_mv(src: str, dst: str) -> None:
    _hdfs_must(["-mv", src, dst])


def hdfs_cp(src: str, dst: str) -> None:
    _hdfs_must(["-cp", src, dst])


def hdfs_ls(path: str) -> List[str]:
    """
    Non-recursive HDFS listing. Returns full paths (best-effort parsing).
    """
    _, out, _ = _hdfs(["-ls", path])
    res: List[str] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        parts = line.split()
        if len(parts) >= 8:
            res.append(parts[-1])
    return res


def hdfs_ls_recursive(path: str) -> List[str]:
    """
    Recursive HDFS listing. Returns full paths (best-effort parsing).
    """
    _, out, _ = _hdfs(["-ls", "-R", path])
    res: List[str] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        if line.endswith(":") and line.startswith("/"):
            continue
        parts = line.split()
        if len(parts) >= 8:
            res.append(parts[-1])
    return res


def list_hdfs_files_with_size(root: str) -> List[Tuple[str, int]]:
    """
    Recursively list files with sizes under root.
    """
    _, out, _ = _hdfs(["-ls", "-R", root])
    res: List[Tuple[str, int]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        if parts[0].startswith("d"):
            continue
        size = int(parts[4])
        path = parts[-1]
        name = path.rsplit("/", 1)[-1]
        if name.startswith("_") or name.startswith("."):
            continue
        res.append((path, size))
    return res


def hdfs_du_bytes(path: str) -> int:
    """
    Return total size in bytes for a path (recursive), 0 if it doesn't exist.
    Uses: hdfs dfs -du -s
    """
    if not hdfs_exists(path):
        return 0
    out = _hdfs_must(["-du", "-s", path])
    # Output: "<bytes>\t<path>" (or spaces). We only need the first token.
    first = out.split()[0]
    return int(first)


def hdfs_get_single_part_file(dir_path: str) -> str:
    """
    Return the first part-*.parquet file inside a Spark output directory.
    Assumes the directory contains exactly one parquet part file (coalesce(1)).
    """
    entries = hdfs_ls(dir_path)
    for p in entries:
        b = p.rsplit("/", 1)[-1]
        if b.startswith("part-") and b.endswith(".parquet"):
            return p
    return ""


def hdfs_finalize_single_file_from_spark_dir(spark_out_dir: str, final_file_path: str) -> None:
    """
    Move the single Spark part file into final_file_path and remove spark_out_dir.
    """
    part_file = hdfs_get_single_part_file(spark_out_dir)
    if not part_file:
        raise RuntimeError("No part-*.parquet found in {}".format(spark_out_dir))

    hdfs_rm_if_exists(final_file_path)
    hdfs_mv(part_file, final_file_path)
    hdfs_rm_if_exists(spark_out_dir, recursive=True)


def hdfs_put_file(local_path: str, hdfs_path: str, overwrite: bool = True) -> None:
    """
    Upload local file to HDFS path.
    If overwrite=True, remove destination first (best effort).
    """
    if overwrite:
        hdfs_rm_if_exists(hdfs_path)
    _hdfs_must(["-put", local_path, hdfs_path])
