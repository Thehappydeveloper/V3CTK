# V3CTK Licensing Compliance Checklist

Use this checklist before merging pull requests.

## 1. Core License Integrity

- [ ] `LICENSE` exists and is GPLv3 text.
- [ ] `README.md` states the dual-license model (GPLv3 + commercial option).
- [ ] No PR changes weaken or remove GPLv3 licensing without explicit owner instruction.

## 2. Dual Licensing Protection

- [ ] Contribution does not introduce terms that block commercial relicensing.
- [ ] New dependencies are reviewed for GPLv3 compatibility.
- [ ] AGPL/restrictive copyleft dependencies are rejected unless explicitly approved.
- [ ] Code provenance is clear; no copied code from unknown/incompatible sources.

## 3. CLA Enforcement

- [ ] External contributor explicitly accepted `CLA.md` before merge.
- [ ] CLA confirmation is recorded in PR discussion or equivalent written channel.

## 4. Proprietary Separation

- [ ] No proprietary/enterprise-only code is merged into the public GPL core.
- [ ] If enterprise features are involved, they are kept on separate private tracks.

## 5. Paper vs Software Licensing

- [ ] Paper/document licenses (e.g., CC-BY) are not applied to software source files unless explicitly intended.
- [ ] Software code in this repository remains GPLv3-governed unless explicitly documented otherwise.

## Decision Rule

If any item is uncertain, pause merge and request explicit project owner approval.
