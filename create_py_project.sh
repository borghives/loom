#!/bin/bash

# Check if a project name is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project-name>"
  exit 1
fi

PROJECT_NAME=$1

# 1. Create a New Project with uv
uv init "$PROJECT_NAME"
cd "$PROJECT_NAME" || exit

# 2. Configure and Install Dependencies
uv add pydantic pyrsistent rich
uv add --dev py-spy pytest

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
