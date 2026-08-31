# ohos-main patch stack

Ordered list of the patches on `ohos-main`, first patch first. Kept in sync
with `git log base/<date>..ohos-main` by `check_stack.py` (subjects must
match exactly). Per entry:

- tier per the fork policy: A = additive, B = seam in shared code,
  C = invasive;
- coverage: what proves the patch after a rebase — `CI` (host build/tests),
  `device` (the arkweb on-device suite), `manual` (only its `Validation:`
  recipe), `tooling` (self-checking scripts), `none` (docs);
- watch: upstream paths the patch depends on but does not modify. `drift.py`
  reports upstream commits touching them, so semantic drift is reviewed
  even when the patch applies cleanly. Keep the list file-level and short.

The checked-in `patches/` directory is generated from the stack by
`gen_patches.py` and verified by `check_stack.py`; never edit it by hand.
Its diff between syncs is the GitHub-viewable form of the range-diff.

1. `docs: Mark this repo as an experimental OHOS downstream fork` — B · none · watch: -
2. `Allow clearly marked AI assisted contributions` — B · none · watch: -
3. `build(ohos): target the API-21 SDK for the OpenHarmony product` — B · manual · watch: support/openharmony python/servo/platform python/servo/package_commands.py
4. `ports/arkweb: Servo as an alternative ArkWeb webview backend` — B · device · watch: components/servo/servo.rs components/servo/webview.rs components/servo/webview_delegate.rs components/shared/embedder ports/servoshell/egl/ohos
5. `ports/arkweb: on-device render fixes, deploy.py, OHOS patches` — A · device · watch: components/servo/webview.rs
6. `ports/arkweb: Wire touch, scroll and key input; fix open/close leaks` — A · device · watch: components/shared/embedder/input_events.rs components/servo/webview.rs
7. `ports/arkweb: Drive console, progress and crash handler callbacks` — A · device · watch: components/servo/webview_delegate.rs
8. `ports/arkweb: Back the cookie manager with Servo's site-data manager` — B · manual · watch: components/net/cookie_storage.rs components/servo/site_data.rs
9. `ports/arkweb: Return ExecuteJavaScript results to ACE` — A · device · watch: components/servo/webview.rs
10. `ports/arkweb: Optional network proxy for on-device testing` — A · device · watch: components/config/prefs.rs
11. `ports/arkweb: restrict libservo_arkweb.so exported symbols` — A · device · watch: -
12. `ports/arkweb: dlopen the shim with RTLD_LOCAL in the OHOS loader` — A · device · watch: -
13. `ports/arkweb: drive presentation from the OHOS display vsync` — A · device · watch: components/servo/servo.rs components/servo/webview.rs
14. `ports/arkweb: honor LibraryLoaded lazy flag, defer Servo::new` — A · device · watch: components/servo/servo.rs
15. `ports/arkweb: split pure conversion helpers into a host-tested module` — B · CI · watch: components/shared/embedder
16. `ports/arkweb: IME / soft-keyboard integration` — B · device · watch: components/shared/embedder/input_events.rs components/servo/webview_delegate.rs ports/servoshell/egl/ohos
17. `ports/arkweb: notify ACE of IME focus via UpdateTextFieldStatus` — A · device · watch: components/servo/webview_delegate.rs
18. `ports/arkweb: add on-device regression tests (scroll, nav, render, multi-webview, vsync)` — B · device · watch: -
19. `ports/arkweb: JS dialogs (alert/confirm/prompt)` — A · device · watch: components/servo/webview_delegate.rs
20. `ports/arkweb: vendor the arkweb-test app + a HAP build script` — A · device · watch: -
21. `ports/arkweb: <select> dropdowns` — A · device · watch: components/servo/webview_delegate.rs
22. `ports/arkweb: <input type=file> pickers` — A · device · watch: components/servo/webview_delegate.rs
23. `ports/arkweb: resolve <input type=color> (no ACE hook)` — A · device · watch: components/servo/webview_delegate.rs
24. `ports/arkweb: wire the OHOS system clipboard via ohos-pasteboard` — B · manual · watch: components/servo/clipboard_delegate.rs components/servo/Cargo.toml
25. `ports/arkweb: sync URL bar on history navigation` — A · device · watch: components/servo/webview_delegate.rs
26. `ports/arkweb: add port README with build/deploy/test docs` — A · none · watch: -
27. `ports/arkweb: wire Tier-1 NWeb methods` — A · device · watch: components/servo/webview.rs components/servo/webview_delegate.rs
28. `ports/arkweb: wire HTTP-auth and permission request delegates` — A · device · watch: components/servo/webview_delegate.rs
29. `storage: add an OHOS RDB backend for webstorage, client_storage and indexeddb` — C · CI+device · watch: components/storage components/config/prefs.rs components/net/Cargo.toml

## Sync log

(none yet — baseline generated retroactively for the 2026-08-29 review)
