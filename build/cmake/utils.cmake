find_program(GCOV_PATH gcov)
find_program(LCOV_PATH lcov)
find_program(GENHTML_PATH genhtml)
find_program(GCOVR_PATH gcovr)
find_program(POWERSHELL_PATH NAMES powershell.exe)
find_program(Python3 python3)
find_program(Simpo NAMES simpo PATHS ${DEPS_DIR}/simpo/bin NO_DEFAULT_PATH)

if (CMAKE_CROSSCOMPILING)
    set(GCOV_PATH "$ENV{GCOV_PATH}")
endif ()

macro(FIND_INCLUDE_DIR result curdir)
    file(GLOB_RECURSE children "${curdir}/*.hpp" "${curdir}/*.h")
    set(dirlist "")
    foreach (child ${children})
        string(REGEX REPLACE "(.*)/.*" "\\1" LIB_NAME ${child})
        if (IS_DIRECTORY ${LIB_NAME})
            list(FIND dirlist ${LIB_NAME} list_index)
            if (${list_index} LESS 0)
                LIST(APPEND dirlist ${LIB_NAME})
            endif ()
        endif ()
    endforeach ()
    set(${result} ${dirlist})
endmacro()

function(setup_coverage)
    if (NOT GCOV_PATH)
        message(FATAL_ERROR "gcov not found! Aborting...")
    else ()
        message(STATUS "gcov found at: ${GCOV_PATH}")
    endif ()

    set(COVERAGE_COMPILER_FLAGS "--coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COVERAGE_COMPILER_FLAGS}" PARENT_SCOPE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_COMPILER_FLAGS}" PARENT_SCOPE)
    message(STATUS "Appending code coverage compiler flags: ${COVERAGE_COMPILER_FLAGS}")
    link_libraries(gcov)

    set(COVERAGE_DIR ${CMAKE_BINARY_DIR}/coverage)
    execute_process(COMMAND mkdir -p ${CMAKE_BINARY_DIR}/coverage
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    )

    if (LCOV_PATH)
        message(STATUS "lcov found in ${LCOV_PATH}.")
        set(HTML_PATH "${COVERAGE_DIR}/index.html")
        add_custom_target(coverage
                COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/build/coverage.sh ${CMAKE_BINARY_DIR}
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                VERBATIM
        )
        add_custom_command(TARGET coverage POST_BUILD
                COMMAND ${LCOV_PATH} -z -d ${CMAKE_BINARY_DIR} # 删除 *.gcda
                COMMENT "Removing intermediate coverage files"
        )
        if (POWERSHELL_PATH)
            message(STATUS "powershell found in ${POWERSHELL_PATH}")
            add_custom_command(TARGET coverage POST_BUILD
                    COMMAND ${POWERSHELL_PATH} /c start build/coverage/index.html;
                    COMMENT "Open ${HTML_PATH} in your browser to view the coverage report."
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            )
        endif ()
        if (ENABLE_HTTP_SERVER)
            add_custom_command(TARGET coverage POST_BUILD
                    COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/build/start_coverage_server.sh ${HTML_PATH}
                    COMMENT "The coverage report has been generated at ${HTML_PATH}."
                    WORKING_DIRECTORY /
                    VERBATIM
            )
        endif ()
    elseif (GCOVR_PATH)
        message(STATUS "gcovr found in ${GCOVR_PATH}.")
        set(HTML_PATH ${CMAKE_BINARY_DIR}/coverage/index.html)
        add_custom_target(coverage
                COMMAND ${GCOVR_PATH} -f src --gcov-exclude '.+log.+' --html-details ${HTML_PATH}
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        )
        if (POWERSHELL_PATH)
            message(STATUS "powershell found in ${POWERSHELL_PATH}")
            add_custom_command(TARGET coverage POST_BUILD
                    COMMAND ${POWERSHELL_PATH} /c start build/coverage/index.html;
                    COMMENT "Open ${HTML_PATH} in your browser to view the coverage report."
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            )
        else ()
            add_custom_command(TARGET coverage POST_BUILD
                    COMMAND ;
                    COMMENT "Open ${HTML_PATH} in your browser to view the coverage report."
            )
        endif ()
    else ()
        message(AUTHOR_WARNING "gcovr not found! replaced by gcov.")
        add_custom_target(coverage
                COMMAND rm -rf *.gcov
                COMMAND find ${CMAKE_BINARY_DIR}/src -name '*.gcda' -exec gcov -pb {} +
                WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/coverage
        )
    endif ()
endfunction()

macro(install_dep dep_name)
    add_custom_target(build_${dep_name}
            COMMAND mkdir -p ${CMAKE_SOURCE_DIR}/deps
            COMMAND rm -rf ${dep_name}/*
            COMMAND tar -xzf ${CMAKE_SOURCE_DIR}/.deps/${dep_name}_aarch64.tar.gz -C ${CMAKE_SOURCE_DIR}/deps
            COMMENT "Install dep ${dep_name} into /deps/${dep_name}."
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    )
endmacro()

function(print_list list)
    foreach (item ${list})
        message(STATUS "${item}")
    endforeach ()
endfunction()

macro(install_lib lib_name)
    file(GLOB ${lib_name}_LIBS "${DEPS_DIR}/${lib_name}/lib/*.so*" "${DEPS_DIR}/${lib_name}/lib64/*.so*" "${DEPS_DIR}/${lib_name}/lib/*.ko")
    file(COPY ${${lib_name}_LIBS} DESTINATION ${CMAKE_BINARY_DIR}/lib/ FILE_PERMISSIONS OWNER_READ OWNER_EXECUTE)
endmacro()

macro(install_lib_cli lib_name)
    file(GLOB ${lib_name}_LIBS "${DEPS_DIR}/${lib_name}/lib/*.so" "${DEPS_DIR}/${lib_name}/lib/*.ko")
    file(COPY ${${lib_name}_LIBS} DESTINATION ${CMAKE_BINARY_DIR}/cli/lib/ FILE_PERMISSIONS OWNER_READ OWNER_EXECUTE)
endmacro()

macro(install_lib_devel lib_name)
    file(GLOB ${lib_name}_LIBS "${DEPS_DIR}/${lib_name}/lib/*.so" "${DEPS_DIR}/${lib_name}/lib/*.ko")
    file(COPY ${${lib_name}_LIBS} DESTINATION ${CMAKE_BINARY_DIR}/devel/lib/ FILE_PERMISSIONS OWNER_READ OWNER_EXECUTE)
endmacro()

# 模块序列化头文件生成
macro(build_message module)
    file(GLOB_RECURSE simpo_files *.simpo)
    if (NOT simpo_files)
        message(WARNING "[${module}] No simpo files have been provided!")
    else ()
        message(STATUS "[${module}] Found simpo files:")
        print_list("${simpo_files}")

        set(header_files "")
        foreach (simpo_file IN LISTS simpo_files)
            # 获取文件名（不带路径和扩展名）
            get_filename_component(base_name ${simpo_file} NAME_WE)

            # 生成目标头文件名
            list(APPEND header_files
                    "${CMAKE_BINARY_DIR}/include/${base_name}_builder.h"
                    "${CMAKE_BINARY_DIR}/include/${base_name}_reader.h"
                    "${CMAKE_BINARY_DIR}/include/${base_name}_verifier.h"
            )
        endforeach ()

        # 添加自定义命令来生成头文件
        add_custom_command(
                OUTPUT ${header_files}
                COMMAND mkdir -p ${CMAKE_BINARY_DIR}/include
                COMMAND ${Simpo} -o ${CMAKE_BINARY_DIR}/include --simpo --cpp ${simpo_files}
                COMMENT "[${module}] Generating interface based on simpo files"
                VERBATIM
                DEPENDS ${simpo_files}  # 依赖于 simpo 文件
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        )

        # 创建一个自定义目标来生成头文件
        add_custom_target(build_${module}_message ALL DEPENDS ${header_files})
    endif ()
endmacro()

# 添加模块 UT
macro(add_ut module)
    set(UT_BINARY ${CMAKE_PROJECT_NAME}_${module}_ut)
    file(GLOB_RECURSE TEST_SOURCES LIST_DIRECTORIES false
            ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/*.h
    )
    add_executable(${UT_BINARY} EXCLUDE_FROM_ALL ${TEST_SOURCES} ${CMAKE_SOURCE_DIR}/test/UT/main.cpp)
    target_compile_definitions(${UT_BINARY} PUBLIC "VOS_OS_VER=VOS_LINUX" "VOS_HARDWARE_PLATFORM=VOS_ARM" "VOS_CPU_TYPE=VOS_CORTEXA72")
    target_link_libraries(${UT_BINARY} PUBLIC
            mockcpp
            googletest
            pthread
            dopra
            ${module}
    )
    # 打破控制权限
    target_compile_options(${UT_BINARY} PRIVATE -fno-access-control ${DEBUG_FLAGS})
    set_target_properties(${UT_BINARY} PROPERTIES LINK_FLAGS "-Wl,--as-needed")
    set(RUN_TEST "${CMAKE_BINARY_DIR}/bin/${UT_BINARY} \
		--gtest_output=xml:${CMAKE_BINARY_DIR}/coverage/${module}_detail.xml")
    # 处理透传参数
    set(TRANS_PARAMS $ENV{TRANS_PARAMS})
    if (DEFINED TRANS_PARAMS AND NOT "${TRANS_PARAMS}" STREQUAL "")
        set(RUN_TEST "${RUN_TEST} ${TRANS_PARAMS}")
    endif ()
    if (SKIP_RUN_TESTS)
        set(RUN_TEST "echo 'Skip run test, only build binary ${CMAKE_BINARY_DIR}/bin/${UT_BINARY}'")
    endif ()
    add_custom_target(${module}_ut
            COMMAND bash -c "${RUN_TEST}" || true
            COMMENT "Run testing: ${RUN_TEST}"
    )
    add_dependencies(${module}_ut ${UT_BINARY})
    gtest_discover_tests(${UT_BINARY} PROPERTIES LABELS "${module}")
endmacro()