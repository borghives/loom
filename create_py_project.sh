#!/bin/bash

# Check if a project name is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project-name>"
  exit 1
fi

PROJECT_NAME=$1

# 1. Create a New Project with Poetry
poetry new "$PROJECT_NAME" --flat
cd "$PROJECT_NAME" || exit

# 2. Configure Poetry and Install Dependencies
poetry config virtualenvs.in-project true
poetry install
poetry add pydantic pyrsistent rich
poetry add --group dev py-spy pytest

# 3. Initialize Git and Create a Repository on GitHub
git init
echo "*/__pycache__/*.pyc
.env
.venv
*.pyc" > .gitignore
git add .
git commit -m "Initial commit"
gh repo create "$PROJECT_NAME" --private --source=. --remote=origin
git push -u origin main

echo "Project '$PROJECT_NAME' created successfully!"
