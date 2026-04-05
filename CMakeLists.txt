cmake_minimum_required(VERSION 3.16)
project(glox_real CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
find_package(OpenSSL REQUIRED)

# ---- Fetch ----
FetchContent_Declare(emp_tool
  GIT_REPOSITORY https://github.com/emp-toolkit/emp-tool.git
  GIT_TAG 0.2.4
)
FetchContent_Declare(emp_ot
  GIT_REPOSITORY https://github.com/emp-toolkit/emp-ot.git
  GIT_TAG 0.2.2
)
FetchContent_Declare(emp_sh2pc
  GIT_REPOSITORY https://github.com/emp-toolkit/emp-sh2pc.git
  GIT_TAG 0.2.2
)

FetchContent_Populate(emp_tool)
FetchContent_Populate(emp_ot)
FetchContent_Populate(emp_sh2pc)

# ---- HARD disable EMP tests by patching test CMakeLists BEFORE add_subdirectory ----
file(WRITE "${emp_tool_SOURCE_DIR}/test/CMakeLists.txt"   "# disabled by glox_real\n")
file(WRITE "${emp_ot_SOURCE_DIR}/test/CMakeLists.txt"     "# disabled by glox_real\n")
file(WRITE "${emp_sh2pc_SOURCE_DIR}/test/CMakeLists.txt"  "# disabled by glox_real\n")

# Also disable any examples dir if it exists (some EMP repos have it)
if(EXISTS "${emp_tool_SOURCE_DIR}/example/CMakeLists.txt")
  file(WRITE "${emp_tool_SOURCE_DIR}/example/CMakeLists.txt" "# disabled by glox_real\n")
endif()

# ---- Add as subprojects ----
add_subdirectory("${emp_tool_SOURCE_DIR}"  "${emp_tool_BINARY_DIR}")
add_subdirectory("${emp_ot_SOURCE_DIR}"    "${emp_ot_BINARY_DIR}")
add_subdirectory("${emp_sh2pc_SOURCE_DIR}" "${emp_sh2pc_BINARY_DIR}")

add_executable(glox_real src/glox_real.cpp)
target_link_libraries(glox_real PRIVATE emp-tool OpenSSL::Crypto)

if(MSVC)
  target_compile_options(glox_real PRIVATE /W4)
else()
  target_compile_options(glox_real PRIVATE -O3 -Wall -Wextra -Wpedantic -maes -mrdseed -march=native)
endif()
