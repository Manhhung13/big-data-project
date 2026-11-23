# ADR-07: Baseline cutoff

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** valid_for_trading_date D must only use train_end < D; manifest versioned.

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
