#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# denoise.py from https://github.com/sgofferj/python-sentinel-pipeline
#
# Copyright Stefan Gofferje
#
# Licensed under the Gnu General Public License Version 3 or higher (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.en.html
#

import numpy as np
from scipy.ndimage import uniform_filter
import os

# --- CUDA Autodetection ---
try:
    import cupy as cp
    from cupyx.scipy.ndimage import uniform_filter as cp_uniform_filter
    HAS_CUDA = os.getenv("DISABLE_GPU", "false").lower() not in ("true", "1")
except ImportError:
    HAS_CUDA = False

def improved_lee_filter(img, size=3):
    """Improved Lee Filter with optional CUDA acceleration."""
    if HAS_CUDA:
        return _improved_lee_cuda(img, size)
    
    img = img.astype(np.float32)
    img_mean = uniform_filter(img, (size, size))
    img_sqr_mean = uniform_filter(img**2, (size, size))
    img_variance = np.maximum(img_sqr_mean - img_mean**2, 0)
    overall_variance = np.var(img)
    
    weighted_variance = img_variance / (img_variance + overall_variance + 1e-9)
    img_lee = img_mean + weighted_variance * (img - img_mean)
    
    std_dev = np.sqrt(img_variance)
    is_outlier = (img > (img_mean + 3 * std_dev))
    img_lee[is_outlier] = img[is_outlier]
    return np.clip(img_lee, 0, np.max(img))

def _improved_lee_cuda(img, size=3):
    """CUDA implementation of Improved Lee Filter."""
    m = cp.get_default_memory_pool()
    img_gpu = cp.array(img, dtype=cp.float32)
    img_mean = cp_uniform_filter(img_gpu, (size, size))
    img_sqr_mean = cp_uniform_filter(img_gpu**2, (size, size))
    img_variance = cp.maximum(img_sqr_mean - img_mean**2, 0)
    overall_variance = cp.var(img_gpu)
    
    weighted_variance = img_variance / (img_variance + overall_variance + 1e-9)
    img_lee = img_mean + weighted_variance * (img_gpu - img_mean)
    
    std_dev = cp.sqrt(img_variance)
    is_outlier = (img_gpu > (img_mean + 3 * std_dev))
    img_lee[is_outlier] = img_gpu[is_outlier]
    
    res = cp.asnumpy(cp.clip(img_lee, 0, cp.max(img_gpu)))
    del img_gpu, img_mean, img_sqr_mean, img_variance, img_lee
    m.free_all_blocks()
    return res

def refined_lee_filter(img, size=5):
    """Refined Lee Filter with optional CUDA acceleration."""
    if HAS_CUDA:
        return _refined_lee_cuda(img, size)
        
    img = img.astype(np.float32)
    img_mean = uniform_filter(img, (size, size))
    img_sqr_mean = uniform_filter(img**2, (size, size))
    img_var = np.maximum(img_sqr_mean - img_mean**2, 0)
    noise_var = np.mean(img_var) * 0.5 
    weights = img_var / (img_var + noise_var + 1e-9)
    img_refined = img_mean + weights * (img - img_mean)
    return np.clip(img_refined, 0, np.max(img))

def _refined_lee_cuda(img, size=5):
    m = cp.get_default_memory_pool()
    img_gpu = cp.array(img, dtype=cp.float32)
    img_mean = cp_uniform_filter(img_gpu, (size, size))
    img_sqr_mean = cp_uniform_filter(img_gpu**2, (size, size))
    img_var = cp.maximum(img_sqr_mean - img_mean**2, 0)
    noise_var = cp.mean(img_var) * 0.5
    weights = img_var / (img_var + noise_var + 1e-9)
    img_refined = img_mean + weights * (img_gpu - img_mean)
    res = cp.asnumpy(cp.clip(img_refined, 0, cp.max(img_gpu)))
    del img_gpu, img_mean, img_var, img_refined
    m.free_all_blocks()
    return res

def frost_filter(img, size=5, damping=2.0):
    """Frost Filter with optional CUDA acceleration."""
    if HAS_CUDA:
        return _frost_cuda(img, size, damping)
        
    img = img.astype(np.float32)
    img_mean = uniform_filter(img, (size, size))
    img_var = np.maximum(uniform_filter(img**2, (size, size)) - img_mean**2, 0)
    cv = np.sqrt(img_var) / (img_mean + 1e-9)
    weights = np.exp(-damping * cv)
    img_frost = img_mean + weights * (img - img_mean)
    return np.clip(img_frost, 0, np.max(img))

def _frost_cuda(img, size=5, damping=2.0):
    m = cp.get_default_memory_pool()
    img_gpu = cp.array(img, dtype=cp.float32)
    img_mean = cp_uniform_filter(img_gpu, (size, size))
    img_var = cp.maximum(cp_uniform_filter(img_gpu**2, (size, size)) - img_mean**2, 0)
    cv = cp.sqrt(img_var) / (img_mean + 1e-9)
    weights = cp.exp(-damping * cv)
    img_frost = img_mean + weights * (img_gpu - img_mean)
    res = cp.asnumpy(cp.clip(img_frost, 0, cp.max(img_gpu)))
    del img_gpu, img_mean, img_var, img_frost
    m.free_all_blocks()
    return res

def gamma_map_filter(img, size=5, looks=1):
    """Gamma Map (MAP) Filter with optional CUDA acceleration."""
    if HAS_CUDA:
        return _gamma_map_cuda(img, size, looks)
        
    img = img.astype(np.float32)
    img_mean = uniform_filter(img, (size, size))
    img_var = np.maximum(uniform_filter(img**2, (size, size)) - img_mean**2, 0)
    ci = np.sqrt(1.0 / looks)
    cu = np.sqrt(img_var) / (img_mean + 1e-9)
    alpha = (1.0 + ci**2) / (cu**2 - ci**2 + 1e-9)
    img_gamma = img_mean * ( (alpha - looks - 1) + np.sqrt( np.maximum( (alpha - looks - 1)**2 + 4 * alpha * looks * img / (img_mean + 1e-9), 0) ) ) / (2 * alpha + 1e-9)
    mask = cu <= ci
    img_gamma[mask] = img_mean[mask]
    point_mask = cu > (ci * 2)
    img_gamma[point_mask] = img[point_mask]
    return np.clip(img_gamma, 0, np.max(img))

def _gamma_map_cuda(img, size=5, looks=1):
    m = cp.get_default_memory_pool()
    img_gpu = cp.array(img, dtype=cp.float32)
    img_mean = cp_uniform_filter(img_gpu, (size, size))
    img_var = cp.maximum(cp_uniform_filter(img_gpu**2, (size, size)) - img_mean**2, 0)
    ci = cp.sqrt(1.0 / looks)
    cu = cp.sqrt(img_var) / (img_mean + 1e-9)
    alpha = (1.0 + ci**2) / (cu**2 - ci**2 + 1e-9)
    img_gamma = img_mean * ( (alpha - looks - 1) + cp.sqrt( cp.maximum( (alpha - looks - 1)**2 + 4 * alpha * looks * img_gpu / (img_mean + 1e-9), 0) ) ) / (2 * alpha + 1e-9)
    img_gamma[cu <= ci] = img_mean[cu <= ci]
    img_gamma[cu > (ci * 2)] = img_gpu[cu > (ci * 2)]
    res = cp.asnumpy(cp.clip(img_gamma, 0, cp.max(img_gpu)))
    del img_gpu, img_mean, img_var, img_gamma
    m.free_all_blocks()
    return res
