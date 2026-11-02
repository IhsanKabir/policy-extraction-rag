# v1c intra-annotator agreement: blind self-relabel (2026-11-02)

Annotator A relabelled 27 of their own gold records blind (no earlier labels, no
heuristic or model output) in the labelling tool, 2026-11-02 10:32-11:41
(`scripts/build_relabel_sample.py`, seed 20261031; `scripts/start_blind_session.ps1 -Session relabel`).
The 27 are every unique labelled record from the first 62 queue positions (the ones
originally labelled with the heuristic pre-filled), so the pool could not be redrawn.

Caveat: 10 of the 27 were also in the IAA sample that A adjudicated with B the same
morning, so A had seen them recently. The 17 others are the cleaner measure.

| Measure | All 27 | 17 not seen in adjudication |
|---|---|---|
| Label vs skip | 26/27 (one record skipped on relabel; kappa undefined, A labelled all) | 17/17 |
| Topic agreement / kappa | 92% / 0.88 | 88% / 0.82 |
| Action agreement / kappa | 85% / 0.65 | 82% / 0.60 |
| Field micro F1 | 0.84 | 0.80 |

Raw: `results/v1c_self_relabel.json`, `results/v1c_self_relabel_unseen.json`.

Reading: A is fairly consistent with themself (topic kappa 0.82, field F1 0.80 on
unseen records) and much more so than with B (topic kappa 0.51, F1 0.52). The
inter-annotator gap is therefore mostly a difference between annotators in applying
the guide, not noise within A. It does not show that A's reading is the right one.
Action is the least stable field for both measures. n is small (17-27).
