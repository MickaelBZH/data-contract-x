# Releasing to PyPI

dcx publishes via **PyPI Trusted Publishing** (GitHub Actions OIDC) — no API
tokens are stored. The flow: tag a version → publish a GitHub Release →
`.github/workflows/release.yml` builds and uploads automatically.

Distribution name: **`datacontract-x`** (the import package and CLI stay `dcx`).

## One-time setup

1. **Push to GitHub** — repo `https://github.com/MickaelBZH/data-contract-x`
   (the package root is this directory).
2. **Register the Trusted Publisher on PyPI.** On https://pypi.org → *Your
   projects* (or *Publishing* for a brand-new project) → add a **pending publisher**:
   - PyPI project name: `datacontract-x`
   - Owner / repository: `MickaelBZH` / `data-contract-x`
   - Workflow name: `release.yml`
   - Environment name: `pypi`
3. *(Optional)* Repeat on https://test.pypi.org for a dry run first.

## Cut a release

```bash
# 1. Bump the version in pyproject.toml ([project].version), commit.
# 2. Tag and push.
git tag v0.1.0
git push origin v0.1.0
# 3. Publish a GitHub Release for that tag (UI or `gh release create v0.1.0 --generate-notes`).
```

Publishing the Release triggers `release.yml`, which builds the sdist + wheel,
runs `twine check`, and uploads to PyPI via OIDC.

## Verify locally before releasing

```bash
python -m pip install --upgrade build twine
python -m build                 # -> dist/*.whl, dist/*.tar.gz
python -m twine check dist/*    # metadata + README render check
pip install dist/*.whl          # smoke-install
dcx info
```

## Manual publish (fallback, not recommended)

If you can't use Trusted Publishing, upload with an API token:

```bash
python -m build
python -m twine upload dist/*    # prompts for __token__ / pypi-... token
```

## Versioning

`[project].version` in `pyproject.toml` is the single source of truth;
`dcx.__version__` reads it from the installed package metadata at runtime. Bump
it for every release (PyPI rejects re-uploading an existing version).
