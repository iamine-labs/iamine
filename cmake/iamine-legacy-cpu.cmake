# Build the daemon against the x86_64 baseline instead of llama.cpp's
# AVX/AVX2 defaults. This file is injected with CMAKE_PROJECT_INCLUDE.
set(GGML_NATIVE OFF CACHE BOOL "Disable native CPU tuning" FORCE)
set(GGML_NATIVE_DEFAULT OFF CACHE BOOL "Disable default native instruction sets" FORCE)
set(GGML_CPU_ALL_VARIANTS OFF CACHE BOOL "Build one baseline CPU backend" FORCE)

set(GGML_SSE42 OFF CACHE BOOL "Disable SSE 4.2 requirement" FORCE)
set(GGML_AVX OFF CACHE BOOL "Disable AVX requirement" FORCE)
set(GGML_AVX2 OFF CACHE BOOL "Disable AVX2 requirement" FORCE)
set(GGML_AVX_VNNI OFF CACHE BOOL "Disable AVX-VNNI requirement" FORCE)
set(GGML_BMI2 OFF CACHE BOOL "Disable BMI2 requirement" FORCE)
set(GGML_FMA OFF CACHE BOOL "Disable FMA requirement" FORCE)
set(GGML_F16C OFF CACHE BOOL "Disable F16C requirement" FORCE)
set(GGML_AVX512 OFF CACHE BOOL "Disable AVX512 requirement" FORCE)
set(GGML_AVX512_VBMI OFF CACHE BOOL "Disable AVX512-VBMI requirement" FORCE)
set(GGML_AVX512_VNNI OFF CACHE BOOL "Disable AVX512-VNNI requirement" FORCE)
set(GGML_AVX512_BF16 OFF CACHE BOOL "Disable AVX512-BF16 requirement" FORCE)
