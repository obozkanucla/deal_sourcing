# PIPELINE_STATE.md

## Purpose

This document defines the **authoritative contract** for the Deal Sourcing pipeline.

It answers, unambiguously:

- What is the source of truth (and when)
- Who is allowed to change what
- How SQLite, Google Sheets, Google Drive, and GitHub interact
- What is SAFE to test locally vs what is PRODUCTION
- How data moves, mutates, and is preserved over time

If behavior is unclear, this document overrides assumptions.

---

## High-level Architecture