# Acier work instructions

- Keep implementations as simple as their supported contracts allow.
- Preserve public behavior unless a change is explicitly requested or reviewed.
- Document public APIs as self-contained contracts: supported values, units,
  defaults, nullability, ordering, ownership and lifetime requirements.
- Keep implementation rationale in source comments or developer documentation.
- Add focused regression coverage for behavior changes and run the relevant tests.
- Keep public headers independently compilable and verify the installed CMake
  package when changing build or export configuration.
- Follow the repository's `.clang-format` rules for C++ sources.
- Preserve license notices and Apache Arrow attribution in derived code.
- Maintain the relevant `docs/components/*.md` comparison when a component changes.
  Identify the exact upstream baseline, separate shared behavior from additions,
  and keep the records free of application-specific history or references.
