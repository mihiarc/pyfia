#!/bin/bash

# PyFIA Documentation Deployment Script

echo "🚀 Deploying PyFIA Documentation..."

# Check if mkdocs is installed
if ! command -v mkdocs &> /dev/null; then
    echo "❌ MkDocs is not installed. Please install it first:"
    echo "   pip install mkdocs mkdocs-material"
    exit 1
fi

# Build the documentation
echo "📚 Building documentation..."
mkdocs build

if [ $? -eq 0 ]; then
    echo "✅ Documentation built successfully!"
    echo "📁 Site files are in: ./site/"
    echo ""
    echo "🌐 To serve locally, run:"
    echo "   mkdocs serve"
    echo ""
    echo "🚀 To deploy to GitHub Pages, run:"
    echo "   mkdocs gh-deploy"
else
    echo "❌ Build failed!"
    exit 1
fi 