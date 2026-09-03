#!/bin/bash
echo "Locating and patching headers in bin/disco-skip..."

# 1. Patch main.hpp files
find bin/disco-skip -name "main.hpp" | while read -r header; do
    if ! grep -q "Wignored-attributes" "$header"; then
        echo "Patching $header"
        sed -i '1i #pragma GCC diagnostic ignored "-Wignored-attributes"' "$header"
    fi
done

# 2. Initialize dependency trackers
echo "Initializing dependency trackers in bin/disco-skip..."
mkdir -p bin/disco-skip/.deps/exports
for lib in conn ctrl shared memory special memstore extern third-party swarm-kv fusee disco-skip; do
    touch "bin/disco-skip/.deps/exports/${lib}.conanbuild"
done

# 3. Patch the Conan Profile
PROFILE_PATH="bin/disco-skip/conan/profiles/gcc-12-relwithdebinfo.profile"
if [ -f "$PROFILE_PATH" ]; then
    ACTUAL_GCC_VERSION=$(gcc -dumpversion | cut -d. -f1)
    sed -i "s/compiler.version=[0-9]*/compiler.version=$ACTUAL_GCC_VERSION/g" "$PROFILE_PATH"
fi