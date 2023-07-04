#!/bin/zsh

#cmake --build build -j$(nproc)
cd build
#make lru_k_replacer_test -j$(nproc)
#make buffer_pool_manager_test -j$(nproc)
make page_guard_test -j$(nproc)

