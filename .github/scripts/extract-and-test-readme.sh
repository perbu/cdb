#!/bin/bash

set -e

echo "🔍 Extracting Go code blocks from README.md..."

# Create a temporary directory for testing
TEST_DIR=$(mktemp -d)
cd "$TEST_DIR"

# Initialize a Go module for testing
echo "📦 Initializing test module..."
go mod init test-readme-examples
go get github.com/perbu/cdb@latest

# Counter for code blocks
BLOCK_COUNT=0

# Extract all Go code blocks from README.md
echo "📝 Processing README.md..."
while IFS= read -r line; do
    if [[ $line == '```go' ]]; then
        BLOCK_COUNT=$((BLOCK_COUNT + 1))
        echo "📋 Found Go code block #$BLOCK_COUNT"
        
        # Create a file for this code block
        CODE_FILE="example_${BLOCK_COUNT}.go"
        
        # Start reading the code block
        while IFS= read -r code_line; do
            if [[ $code_line == '```' ]]; then
                break
            fi
            echo "$code_line" >> "$CODE_FILE"
        done
        
        # Try to compile this code block
        echo "🔨 Compiling $CODE_FILE..."
        if go build "$CODE_FILE"; then
            echo "✅ $CODE_FILE compiled successfully"
            rm -f "$(basename "$CODE_FILE" .go)"  # Remove the binary
        else
            echo "❌ $CODE_FILE failed to compile"
            echo "Content of $CODE_FILE:"
            cat "$CODE_FILE"
            exit 1
        fi
        
        echo ""
    fi
done < "${GITHUB_WORKSPACE}/README.md"

echo "🎉 All $BLOCK_COUNT Go code blocks compiled successfully!"

# Cleanup
cd /
rm -rf "$TEST_DIR"