#!/bin/bash

# Extract test cases from group arrays in a file
extract_test_cases() {
	local file="$1"
	# Extract test names between single quotes
	grep -o "'[^']*'" "$file" | tr -d "'" | tr ' ' '\n' | grep -v '^$' | sort -u
}

# Check if all test cases are covered and exist
check_test_coverage() {
	local cur_dir="$1"
	local light_file="$cur_dir/run_light_it_in_ci.sh"
	local heavy_file="$cur_dir/run_heavy_it_in_ci.sh"

	# Get all test directories
	local tmp_all_tests=$(mktemp)
	while IFS= read -r -d '' dir; do
		base_dir=$(basename "$dir")
		if [ "$base_dir" != "_utils" ] && [ "$base_dir" != "_certificates" ] && [ -f "$dir/run.sh" ]; then
			echo "$base_dir" >>"$tmp_all_tests"
		fi
	done < <(find "$cur_dir" -mindepth 1 -maxdepth 1 -type d -print0)
	sort -u "$tmp_all_tests" -o "$tmp_all_tests"

	# Create a temporary file to store unique test cases from both scripts
	local tmp_found_tests=$(mktemp)
	# Get test cases from both files
	{
		extract_test_cases "$light_file"
		extract_test_cases "$heavy_file"
	} | sort -u >"$tmp_found_tests"

	# Find missing test cases (those that exist but aren't in any group)
	local missing_tests=()
	while IFS= read -r test; do
		if ! grep -Fxq "$test" "$tmp_found_tests"; then
			missing_tests+=("$test")
		fi
	done <"$tmp_all_tests"

	# Find non-existent test cases (those that are in groups but don't exist)
	local nonexistent_tests=()
	while IFS= read -r test; do
		if ! grep -Fxq "$test" "$tmp_all_tests"; then
			nonexistent_tests+=("$test")
		fi
	done <"$tmp_found_tests"

	# Clean up temporary files
	rm -f "$tmp_all_tests" "$tmp_found_tests"

	# Report issues
	local has_error=0

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
		echo
		#TODO: add this "has_error=1" when we have added all the missing test cases to the groups
		# has_error=1
	fi

	if [ ${#nonexistent_tests[@]} -gt 0 ]; then
		total_nonexistent=${#nonexistent_tests[@]}
		echo "Error: Found $total_nonexistent test cases in groups that don't exist in $cur_dir:"
		printf '%s\n' "${nonexistent_tests[@]}" | sort -u
		echo
		echo "Please remove these non-existent test cases from the groups in:"
		echo "1. run_light_it_in_ci.sh"
		echo "2. run_heavy_it_in_ci.sh"
		echo
		has_error=1
	fi

	if [ $has_error -eq 1 ]; then
		exit 1
	fi
}
