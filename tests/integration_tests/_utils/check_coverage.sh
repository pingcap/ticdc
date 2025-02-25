#!/bin/bash

# Normalize test case names by removing quotes and extra whitespace
normalize_test_name() {
	echo "$1" | tr -d "'" | tr -s ' ' '\n' | grep -v '^$'
}

# Extract test cases from group arrays in a file
extract_test_cases() {
	local file="$1"
	# Extract test names between single quotes
	grep -o "'[^']*'" "$file" | tr -d "'" | tr ' ' '\n' | grep -v '^$' | sort -u
}

# Check if all test cases are covered
check_test_coverage() {
	local cur_dir="$1"
	local light_file="$cur_dir/run_light_it_in_ci.sh"
	local heavy_file="$cur_dir/run_heavy_it_in_ci.sh"

	# Get all test directories
	local all_test_dirs=()
	while IFS= read -r -d '' dir; do
		base_dir=$(basename "$dir")
		if [ "$base_dir" != "_utils" ] && [ "$base_dir" != "_certificates" ] && [ -f "$dir/run.sh" ]; then
			all_test_dirs+=("$base_dir")
		fi
	done < <(find "$cur_dir" -mindepth 1 -maxdepth 1 -type d -print0)

	# Create a temporary file to store unique test cases
	local tmp_all_tests=$(mktemp)
	local tmp_found_tests=$(mktemp)

	# Store sorted list of all test directories
	printf "%s\n" "${all_test_dirs[@]}" | sort >"$tmp_all_tests"

	# Extract and combine test cases from both files
	{
		extract_test_cases "$light_file"
		extract_test_cases "$heavy_file"
	} | sort -u >"$tmp_found_tests"

	# Find missing test cases
	local missing_tests=()
	while IFS= read -r test; do
		if ! grep -Fxq "$test" "$tmp_found_tests"; then
			missing_tests+=("$test")
		fi
	done <"$tmp_all_tests"

	# Clean up temporary files
	rm -f "$tmp_all_tests" "$tmp_found_tests"

	# Report missing test cases
	if [ ${#missing_tests[@]} -gt 0 ]; then
		total_missing=${#missing_tests[@]}
		echo "Warning: Found $total_missing test cases that are not covered by any group:"
		printf '%s\n' "${missing_tests[@]}" | sort -u
		echo
		echo "These test cases need to be added to appropriate groups in:"
		echo "1. run_light_it_in_ci.sh - for light test cases"
		echo "2. run_heavy_it_in_ci.sh - for heavy test cases"
		echo
		echo "Choose the file based on the test case's resource requirements."
		#TODO: add this "exit 1" when we have added all the missing test cases to the groups
		# exit 1
	fi
}
