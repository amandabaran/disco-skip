cmake_minimum_required(VERSION 3.9)
project(chimera CXX)

# 1. Macro Toggles (1 to Enable, 0 to Disable)
set(CHIMERA_CACHE_ENABLED 1)
set(CHIMERA_WRITEBACK_ENABLED 0)

# 2. Pass these variables into your C++ code as preprocessor definitions
add_compile_definitions(CHIMERA_CACHE_ENABLED=${CHIMERA_CACHE_ENABLED})
add_compile_definitions(CHIMERA_WRITEBACK_ENABLED=${CHIMERA_WRITEBACK_ENABLED})

# 3. Relax GCC 13 strict warning checks to fix the compilation errors
if(CMAKE_COMPILER_IS_GNUCXX)
    add_compile_options(
        -Wno-error=switch-default 
        -Wno-error=conversion 
        -Wno-error=missing-declarations
        -Wno-error=undef
    )
endif()

include(${CMAKE_BINARY_DIR}/setup.cmake)
dory_setup_cmake()

add_executable(chimera ${HEADER_TIDER} main.cpp)

target_link_libraries(chimera ${CONAN_LIBS})
