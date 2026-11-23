# ADR-06: Baseline sample window

**Status:** PROPOSED (not stakeholder-approved).

**Pilot rule:** 60 prior trading dates, >=20 observations; sample std; sparse means unknown.

**Decision gate:** Approve after matching a real source payload, exchange calendar or measured acceptance test as relevant.

**Risk:** Changing this rule affects result reproducibility, data contracts and regression fixtures. Increment formula/mapping/schema versions if semantics change.
