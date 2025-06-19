#!/bin/bash
# Setup script for new pyFIA repository

echo "Setting up pyFIA repository..."

# Option 1: If you want to push the current state as-is
echo "Option 1: Push current branch to new repository"
echo "git push -u origin master"

# Option 2: If you want to create a new main branch (GitHub default)
echo -e "\nOption 2: Create and push to main branch"
echo "git checkout -b main"
echo "git push -u origin main"

# Option 3: If you want to start fresh with just pyFIA files
echo -e "\nOption 3: Create a clean pyFIA-only repository"
echo "# First, create a new branch with only pyFIA content"
echo "git checkout --orphan pyfia-clean"
echo "git rm -rf ."
echo "git add pyFIA/"
echo "git add COMPLETE_RFIA_ESTIMATES.md VALIDATION_RESULTS_SUMMARY.md"
echo "git commit -m 'Initial pyFIA implementation with validation'"
echo "git push -u origin pyfia-clean:main"

echo -e "\nNote: Make sure to create the repository on GitHub first:"
echo "https://github.com/new"
echo "Repository name: pyfia"