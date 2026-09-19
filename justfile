build_dir := "build"

# List available recipes
default:
    @just --list

# Configure and build the library, the CLI and the tests. Configuring is
# skipped once the build directory exists, so changing a CMake option means
# `just clean` first.
build:
    @test -d {{build_dir}} || cmake -S . -B {{build_dir}} -DCMAKE_BUILD_TYPE=Release
    @cmake --build {{build_dir}} --parallel

# Run the test suite
test: build
    ctest --test-dir {{build_dir}} --output-on-failure

# Run the test cases whose name matches a pattern, e.g. `just test-only Segment`
# cd into build/test first: the segmenting-splitter tests load their model by
# a path relative to the binary's own directory (test/test_segment.cc's
# MODEL_PATH), same as running the binary directly would.
test-only pattern: build
    cd {{build_dir}}/test && ./test-main --gtest_filter="*{{pattern}}*"

# Run the query benchmark. `repeat` concatenates the KJV corpus that many
# times under fresh document ids, lengthening every postings list without
# growing the vocabulary; 10 is the scale the optimization work was measured
# at. cd into build/bench first: the corpus path is relative to the binary's
# own directory, same as the tests (see bench/bench.cpp).
bench repeat="10": build
    cd {{build_dir}}/bench && ./searchlib-bench --repeat {{repeat}}

# Update vendored third-party code (third_party/, see its README). With no
# argument, checks and updates fstlib, unicodelib, peglib and segmentlib in
# turn; pass one of those names to update just it. Never commits -- review
# the diff, rebuild, and `just test` before committing (see
# scripts/vendor_update_*.sh for what each one tracks and why).
vendor-update *name:
    scripts/vendor_update.sh {{name}}

# Remove the build directory
clean:
    rm -rf {{build_dir}}

# Release a new version (dry run by default; `just release --run` to publish)
release *args:
    @./scripts/release.sh {{args}}
