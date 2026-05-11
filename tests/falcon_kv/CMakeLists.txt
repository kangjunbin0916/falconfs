include(GoogleTest)

enable_testing()

# ==================== FalconKVPrimitivesUT =================
add_executable(FalconKVPrimitivesUT
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_bitmap_allocator.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_lease_manager.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_lru_manager.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_metadata_engine.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_metadata_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_dram_pool.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_store_region_registry.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_ssd_spill_manager.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_store_engine.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_data_service_impl.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_brpc_adapters.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_throttle_admission.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_subtx.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_meta_table_accessor.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_kv_metadata_recovery.cpp
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/test_eviction_coordinator.cpp
)

target_include_directories(FalconKVPrimitivesUT PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVPrimitivesUT
    FalconKVCommon
    FalconKVMetadataService
    FalconKVDataService
    FalconKVBrpcServiceAdapters
    FalconKVProto
    gtest
    gtest_main
    pthread
)

gtest_discover_tests(FalconKVPrimitivesUT)

# ==================== FalconKVMetadataBrpcE2E =================
add_executable(FalconKVMetadataBrpcE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_metadata_brpc_e2e_client.cpp
)

target_include_directories(FalconKVMetadataBrpcE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVMetadataBrpcE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)

# ==================== FalconKVMetaEngineConcurrencyE2E =================
add_executable(FalconKVMetaEngineConcurrencyE2E
    ${PROJECT_SOURCE_DIR}/tests/falcon_kv/kv_metadata_meta_engine_concurrency_e2e.cpp
)

target_include_directories(FalconKVMetaEngineConcurrencyE2E PRIVATE
    ${FALCON_KV_INCLUDE_DIRS}
)

target_link_libraries(FalconKVMetaEngineConcurrencyE2E
    FalconKVProto
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
    pthread
)
