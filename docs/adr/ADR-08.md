# ADR-08: Anomaly threshold

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** z>=3 or P99 if observed>mean and min_count; calibration on real data pending.

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
