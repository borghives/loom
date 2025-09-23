# Plan for Publishing Python Module to PyPI

This plan outlines the steps to publish your Python module so it can be installed via `pip`.

### 1. Introduction

The goal is to package the `loom` module and publish it on the Python Package Index (PyPI), making it publicly available for `pip` installation.

### 2. Prerequisites

- **Install necessary tools**: You'll need `build` to create the package files and `twine` to upload them.
  ```bash
  pip install build twine
  ```

- **Create PyPI Accounts**: You need accounts on both the test and official PyPI servers.
  - [TestPyPI](https://test.pypi.org/account/register/)
  - [PyPI](https://pypi.org/account/register/)

- **Generate API Tokens**: For security, it's best to use API tokens to upload your package. Generate one for TestPyPI and one for PyPI. When you generate them, you can copy them to a safe place.

### 3. Configure `pyproject.toml`

Your `pyproject.toml` file is the heart of your package's configuration. You'll need to ensure it has the necessary metadata for PyPI.

- **`[tool.poetry]` section**: This is where you define your package's metadata.
  - `name`: The name of your package on PyPI (e.g., "loom-your-username"). This must be unique.
  - `version`: The version of your package (e.g., "0.1.0").
  - `description`: A short description of your package.
  - `authors`: Your name and email.
  - `license`: The license for your package (e.g., "MIT").
  - `readme`: The path to your README file (e.g., "README.md").
  - `homepage`: A link to your project's homepage (e.g., your GitHub repo).
  - `repository`: A link to your project's repository.
  - `keywords`: A list of keywords for your package.

### 4. Build the Package

Once your `pyproject.toml` is configured, you can build your package. This will create a `dist` directory with the files to be uploaded.

```bash
python -m build
```

This command will create two files in the `dist` directory:
- A source archive (`.tar.gz`)
- A wheel (`.whl`)

### 5. Upload to TestPyPI

Before publishing to the official PyPI, it's a good practice to upload to TestPyPI to make sure everything works as expected.

```bash
twine upload --repository testpypi dist/*
```

`twine` will prompt you for your TestPyPI username and password. For the password, use the API token you generated.

### 6. Test Installation from TestPyPI

You can test installing your package from TestPyPI using the following command:

```bash
pip install --index-url https://test.pypi.org/simple/ your-package-name
```

Replace `your-package-name` with the name you specified in `pyproject.toml`.

### 7. Upload to PyPI

Once you've confirmed that everything works on TestPyPI, you can upload your package to the official PyPI.

```bash
twine upload dist/*
```

Again, `twine` will prompt for your username and password. Use your PyPI username and the API token you generated for PyPI.

### 8. Tagging a Release (Optional but Recommended)

It's a good practice to create a Git tag for each release of your package.

```bash
git tag v0.1.0
git push origin v0.1.0
```

This helps users and contributors to find specific versions of your code.
