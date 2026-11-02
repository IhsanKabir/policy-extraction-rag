# Data licence

## Annotations: CC BY 4.0

The labels, adjudication decisions, agreement samples, annotation guide
(`docs/LABELING_GUIDE.md`) and the systems' output files in this folder are
Copyright (c) 2026 Ihsan Kabir, licensed under the Creative Commons Attribution
4.0 International licence (CC BY 4.0): https://creativecommons.org/licenses/by/4.0/

You may share and adapt them for any purpose, provided you give appropriate
credit, link to the licence, and indicate if changes were made. Please cite the
paper (see README.md).

## Airline clause text: not covered by this licence

The `clause_text` and `chunk_text` fields (in `clauses.jsonl`, the label files,
the samples and the prediction files) are excerpts of pages published by the
airlines listed in `sources.csv`. Copyright in that text remains with each
airline. It is included only so that the annotations can be read and the results
reproduced, for non-commercial research use, with attribution to the source
pages in `sources.csv`. Neither this licence nor the MIT licence of the code
grants any rights in it.

Any airline that wants its text removed can open an issue at
https://github.com/IhsanKabir/policy-extraction-rag/issues and it will be taken
out of the next release.
