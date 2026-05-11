/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * KV cache GUCs (v6 §14, §27). Owned by `falcon.so`, registered through
 * `DefineCustomIntVariable` in `falcon_init.c`, and read by the in-plugin
 * eviction worker in `libbrpcplugin.so` after `dlopen`. The globals are
 * exported with default visibility so the plugin can resolve them.
 */
#ifndef FALCON_CONNECTION_POOL_FALCON_KV_CONFIG_H
#define FALCON_CONNECTION_POOL_FALCON_KV_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

/* Eviction thread cadence in milliseconds (clamped 50..60000 in plugin). */
#define FALCON_KV_EVICTION_PERIOD_MS_DEFAULT 200
extern int FalconKvEvictionPeriodMs;

/* Per-region free-blocks ratio (in percent, 0..100) below which the eviction
 * worker switches to aggressive mode (skip the period wait and immediately
 * run another cycle). */
#define FALCON_KV_EVICTION_LOW_WATERMARK_PCT_DEFAULT 10
extern int FalconKvEvictionLowWatermarkPct;

/* Maximum cold candidates handled per eviction cycle (clamped 1..1024). */
#define FALCON_KV_EVICTION_CHUNK_DEFAULT 64
extern int FalconKvEvictionChunk;

#ifdef __cplusplus
}
#endif

#endif  // FALCON_CONNECTION_POOL_FALCON_KV_CONFIG_H
