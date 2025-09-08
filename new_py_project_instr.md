# Instructions for Creating a New Python Project

## 1. Create a New Project with Poetry

- Create a new project using `poetry new`.
  ```bash
  poetry new my-new-project --flat
  cd my-new-project
  ```

## 2. Configure Poetry and Install Dependencies

- Configure Poetry to create a virtual environment in the project's root directory:
  ```bash
  poetry config virtualenvs.in-project true
  ```
- Install the base dependencies. This will create the `.venv` directory.
  ```bash
  poetry install
  ```
- Add the dependencies from this project to your new project:
  ```bash
  poetry add pydantic pyrsistent rich
  ```
- Add the development dependencies:
  ```bash
  poetry add --group dev py-spy pytest
  ```

## 3. Initialize Git and Create a Repository on GitHub

- Initialize a new Git repository:
  ```bash
  git init
  ```
- Create a `.gitignore` file:
  ```bash
  echo "*/__pycache__/*.pyc\n.env\n.venv" > .gitignore
  ```

- Add the new files to git and create an initial commit:
  ```bash
  git add .gitignore pyproject.toml poetry.lock
  git commit -m "Initial commit"
  ```
- Create a new repository on GitHub and push the initial commit:
  ```bash
  gh repo create my-new-project --public --source=. --remote=origin
  git push -u origin main
  ```