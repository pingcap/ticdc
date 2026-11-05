# Testing Guidelines

Read this before adding or changing tests.

- Unit tests: `*_test.go`, favor deterministic tests; use `testify/require`.
- Unit tests should cover meaningful behavior only; avoid redundant or low-value cases.
- Do not test across feature boundaries. Keep each test focused on the behavior owned by the package or component under test.
- Reuse existing tests when possible. Add a new test only when reuse would make the existing test unclear or incomplete.
- If several test functions are highly related, merge them into one concise table-driven or scenario-based test.
- When a test needs mocked components, prefer existing gomock-generated mocks over handwritten mocks. If the required mock does not exist, add it to the mock generation flow and run `make generate_mock`.
- Keep tests efficient, simple, focused, and easy to update.
- Failpoints: `make unit_test` enables/disables automatically. If you enable manually, disable before committing to avoid a dirty tree.
- For documentation-only changes, unit tests are usually unnecessary. If tests are skipped, state in the final response that only documentation was changed.
