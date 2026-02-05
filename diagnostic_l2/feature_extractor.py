# diagnostic_l2/feature_extractor.py

def extract_features(job: dict) -> dict:
    """
    L1 Feature Extraction
    Input  : raw job from runner
    Output : feature dict for L2 diagnostic
    """

    acc = job.get("acceleration")
    if not acc:
        return {}

    # ===== PLACEHOLDER =====
    # ganti ini nanti dengan FFT / envelope / DSP real
    return {
        "overall_vel_rms_mm_s": job.get("overall_vel_rms_mm_s"),
        "envelope_rms": job.get("envelope_rms"),
        # future:
        # "kurtosis_env": ...
        # "bpfi_energy": ...
        # "bpfo_energy": ...
    }
