"""Validate source inventory without importing cases, mounting, or running workloads."""

import ast
import hashlib
import json
import pathlib
import subprocess
import sys

BASE = pathlib.Path(__file__).resolve().parent


def exports(tree):
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, (ast.ImportFrom, ast.Import)):
            names.update(a.asname or a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.Assign):
            names.update(
                n.id
                for t in node.targets
                for n in ast.walk(t)
                if isinstance(n, ast.Name)
            )
    return names


def main():
    trees = {}
    for path in BASE.rglob("*.py"):
        source = path.read_text()
        compile(source, str(path), "exec")
        trees[path] = ast.parse(source)
    # Verify imports of bundled helpers without executing their module-level code.
    for path, tree in trees.items():
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or not node.module:
                continue
            candidates = [
                path.parent / (node.module + ".py"),
                BASE / "site" / (node.module + ".py"),
            ]
            module = next((p for p in candidates if p in trees), None)
            if module:
                missing = {a.name for a in node.names} - exports(trees[module])
                assert not missing, (str(path), node.module, sorted(missing))
    shells = sorted(BASE.rglob("*.sh"))
    for path in shells:
        subprocess.run(["bash", "-n", str(path)], check=True)
    manifest = json.loads((BASE / "case-manifest.json").read_text())
    matrix = json.loads((BASE / "matrix.json").read_text())
    assert len(manifest) == 51 and len(matrix) == 306
    assert len({(c["suite"], c["id"]) for c in manifest}) == 51
    assert len({(c["suite"], c["case"], c["group"]) for c in matrix}) == 306
    for suite, count in [("A", 60), ("B", 108), ("C", 138)]:
        assert sum(c["suite"] == suite for c in matrix) == count
    for item in manifest:
        path = BASE / item["script"]
        assert path in trees, str(path)
        if item["suite"] != "A":
            ids = [
                n.args[1].value
                for n in ast.walk(trees[path])
                if isinstance(n, ast.Call)
                and isinstance(n.func, ast.Name)
                and n.func.id == "main_guard"
                and len(n.args) > 1
                and isinstance(n.args[1], ast.Constant)
            ]
            assert ids == [item["id"]], (path, ids)
        selected = [
            r for r in matrix if r["suite"] == item["suite"] and r["case"] == item["id"]
        ]
        assert {r["group"] for r in selected} == set(item["groups"])
    for suite, entry in [
        ("A", "sixway/run_sixway_vm.py"),
        ("B", "site/run_all.py"),
        ("C", "site/run_dn.py"),
    ]:
        proc = subprocess.run(
            [sys.executable, "-B", str(BASE / entry), "--list"],
            check=True,
            capture_output=True,
            text=True,
        )
        listed = json.loads(proc.stdout)
        expected = [r for r in matrix if r["suite"] == suite]
        if suite == "A":
            assert {(r["case"], r["group"]) for r in listed} == {
                (r["case"], r["group"]) for r in expected
            }
        else:
            assert set(listed) == {pathlib.Path(r["script"]).name for r in expected}
    for summary, suite in [("site/summarize.py", "B"), ("site/summarize_dn.py", "C")]:
        tree = trees[BASE / summary]
        order = next(
            ast.literal_eval(n.value)
            for n in tree.body
            if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "ORDER" for t in n.targets)
        )
        assert {r[0] for r in order} == {
            r["id"] for r in manifest if r["suite"] == suite
        }
    node_manifest = json.loads((BASE / "node_fs/manifest.json").read_text())
    assert node_manifest["node_version"] == "v22.13.0"
    assert node_manifest["counts"] == {"parallel": 240, "sequential": 4}
    assert len(node_manifest["tests"]) == 244
    assert len(set(node_manifest["tests"])) == 244
    listed = subprocess.run(
        [sys.executable, "-B", str(BASE / "node_fs/run_test_fs.py"), "--list"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert len(json.loads(listed.stdout)) == 244
    faults = json.loads((BASE / "faults/manifest.json").read_text())
    assert [item["id"] for item in faults["cases"]] == ["x1", "x2", "x3", "x4", "x5"]
    fault_list = subprocess.run(
        [sys.executable, "-B", str(BASE / "faults/run_x.py"), "--list"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(fault_list.stdout) == {
        item["id"]: item["evidence"] for item in faults["cases"]
    }
    for line in (BASE / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        assert hashlib.sha256((BASE / name).read_bytes()).hexdigest() == digest, name
    print(
        json.dumps(
            dict(
                python_files=len(trees),
                shell_files=len(shells),
                cases=51,
                execution_rows=306,
                node_official_tests=244,
                gcp_fault_cases=5,
                syntax="pass",
                local_imports="pass",
                runner_lists="pass",
                summary_coverage="pass",
                checksums="pass",
                live_tests="not run",
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
