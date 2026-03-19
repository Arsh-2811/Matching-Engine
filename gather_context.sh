#!/bin/bash

OUTPUT_FILE="codebase_context.txt"

> "$OUTPUT_FILE"

echo "Gathering .cpp and .hpp files for context..."

find . \( -name ".git" -o -name "build" \) -prune -o -type f \( -name "*.cpp" -o -name "*.hpp" \) -print | sort | while read -r file; do
    
    clean_path="${file#./}"
    
    echo "Adding: $clean_path"
    
    echo "================================================================================" >> "$OUTPUT_FILE"
    echo "FILE: $clean_path" >> "$OUTPUT_FILE"
    echo "================================================================================" >> "$OUTPUT_FILE"
    
    cat "$file" >> "$OUTPUT_FILE"
    
    echo -e "\n\n" >> "$OUTPUT_FILE"

done

echo "Done! Codebase context successfully saved to $OUTPUT_FILE"