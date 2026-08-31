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

1. `docs: add the ohos-main sync runbook and stack manifest` — A · tooling · watch: -
2. `docs: Mark this repo as an experimental OHOS downstream fork` — B · none · watch: -
3. `Allow clearly marked AI assisted contributions` — B · none · watch: -
4. `build(ohos): target the API-21 SDK for the OpenHarmony product` — B · manual · watch: support/openharmony python/servo/platform python/servo/package_commands.py
5. `ports/arkweb: Servo as an alternative ArkWeb webview backend` — B · device · watch: components/servo/servo.rs components/servo/webview.rs components/servo/webview_delegate.rs components/shared/embedder ports/servoshell/egl/ohos
6. `ports/arkweb: on-device render fixes, deploy.py, OHOS patches` — A · device · watch: components/servo/webview.rs
7. `ports/arkweb: Wire touch, scroll and key input; fix open/close leaks` — A · device · watch: components/shared/embedder/input_events.rs components/servo/webview.rs
8. `ports/arkweb: Drive console, progress and crash handler callbacks` — A · device · watch: components/servo/webview_delegate.rs
9. `ports/arkweb: Back the cookie manager with Servo's site-data manager` — B · manual · watch: components/net/cookie_storage.rs components/servo/site_data.rs
10. `ports/arkweb: Return ExecuteJavaScript results to ACE` — A · device · watch: components/servo/webview.rs
11. `ports/arkweb: Optional network proxy for on-device testing` — A · device · watch: components/config/prefs.rs
12. `ports/arkweb: restrict libservo_arkweb.so exported symbols` — A · device · watch: -
13. `ports/arkweb: dlopen the shim with RTLD_LOCAL in the OHOS loader` — A · device · watch: -
14. `ports/arkweb: drive presentation from the OHOS display vsync` — A · device · watch: components/servo/servo.rs components/servo/webview.rs
15. `ports/arkweb: honor LibraryLoaded lazy flag, defer Servo::new` — A · device · watch: components/servo/servo.rs
16. `ports/arkweb: split pure conversion helpers into a host-tested module` — B · CI · watch: components/shared/embedder
17. `ports/arkweb: IME / soft-keyboard integration` — B · device · watch: components/shared/embedder/input_events.rs components/servo/webview_delegate.rs ports/servoshell/egl/ohos
18. `ports/arkweb: notify ACE of IME focus via UpdateTextFieldStatus` — A · device · watch: components/servo/webview_delegate.rs
19. `ports/arkweb: add on-device regression tests (scroll, nav, render, multi-webview, vsync)` — B · device · watch: -
20. `ports/arkweb: JS dialogs (alert/confirm/prompt)` — A · device · watch: components/servo/webview_delegate.rs
21. `ports/arkweb: vendor the arkweb-test app + a HAP build script` — A · device · watch: -
22. `ports/arkweb: <select> dropdowns` — A · device · watch: components/servo/webview_delegate.rs
23. `ports/arkweb: <input type=file> pickers` — A · device · watch: components/servo/webview_delegate.rs
24. `ports/arkweb: resolve <input type=color> (no ACE hook)` — A · device · watch: components/servo/webview_delegate.rs
25. `ports/arkweb: wire the OHOS system clipboard via ohos-pasteboard` — B · manual · watch: components/servo/clipboard_delegate.rs components/servo/Cargo.toml
26. `ports/arkweb: sync URL bar on history navigation` — A · device · watch: components/servo/webview_delegate.rs
27. `ports/arkweb: add port README with build/deploy/test docs` — A · none · watch: -
28. `ports/arkweb: wire Tier-1 NWeb methods` — A · device · watch: components/servo/webview.rs components/servo/webview_delegate.rs
29. `ports/arkweb: wire HTTP-auth and permission request delegates` — A · device · watch: components/servo/webview_delegate.rs
30. `storage: add an OHOS RDB backend for webstorage, client_storage and indexeddb` — C · CI+device · watch: components/storage components/config/prefs.rs components/net/Cargo.toml

31. `docs: document how to update local branches after a sync` — B · none · watch: -

## Sync log

One entry per sync, newest first. Records the upstream range and every
patch that was dropped, superseded, squashed or split, with the reason.

### 2026-08-29

- base: `base/2026-08-07` (79e2c7a8827) → `base/2026-08-29` (bc6153bb1bb)
- upstream: 297 commits; stack 29 → 30 (new first patch: this runbook + manifest)
- conflicts: `ports/arkweb: wire the OHOS system clipboard via ohos-pasteboard`
  (servo-crate half now upstream 5c2a3839525 / #47177, patch shrunk to the
  arkweb glue, `clipboard` feature, READ_PASTEBOARD test-app permission;
  not a full supersede since upstream declined clipboard read);
  `storage: add an OHOS RDB backend …` (Cargo.toml vs #47198/#43819,
  `ascii_serialization` → `Cow` from #47264 mirrored in the twins).
- fold: nothing to do (no fixups pending; the clipboard shrink's empty-
  placeholder rule did not trigger — the patch shrank, it did not empty)
- dropped: none
- known stale: the RDB commit's 1.91 MiB RDB-only size saving no longer
  holds (#43819 makes rusqlite unconditional in components/net); remeasure.
- hygiene items: pre-sync `Cargo.lock` had a dangling `cookie 0.18.1` in the
  arkweb entry (`--locked` broken mid-stack); duplicated proxy comment in
  `ports/arkweb/src/runtime.rs`; three squashed commits keep an old
  `Validation:`/`AI-assisted:` pair mid-body.
- drift (from `drift.py`; hits not listed had no verdict-worthy content):
  - `b780274a164` clang-19, `058c55ed4a1` cargo-ohos build env → patch 4
    (API-21) unaffected textually; the rewritten packaging path is proven
    by the servoshell OHOS release builds. `./mach package --ohos` signing
    fails in the container (hvigor 00303116) — container config, not drift.
  - `c1517a950cc` ohos keyboard support → servoshell + xcomponent-sys only,
    no code shared with the arkweb IME patches (17, 18); device IME tests
    pass.
  - `27775d1d904` queued session-history traversals → potential behaviour
    change for patches 26 (URL bar follows history) and 28 (back/forward
    Tier-1 methods); `test_urlbar.py`, `test_controller.py` pass.
  - `6ee336ce836` MouseButton(s), `1728d63f488` a11y bounds,
    `522fa371e41` webgl guard, `a40c8f29010` threadboost, `e2bcea2a0f6`
    DPR override → embedder-API surface of patches 5, 7, 14–16 changed
    shape; `./mach check --ohos -p arkweb` warning-free and the device suite
    (render, scroll, vsync) passes. No behaviour drift observed.
  - `7a27057feec` cookie/auth memory reporting → patch 9 (cookie manager)
    compiles against unchanged `cookie_storage.rs` API; its manual
    Validation was NOT run this sync (no automated coverage) — open.
  - `cb680edc199` removed prefs, `8067519db8c`/`76f48dc9e3a`/`fb04f8f1613`
    pref additions → patches 11 and 30 set prefs by field name; compile
    proves none of the removed prefs were used.
  - `723dd93c61e`/`22b7b75ba42`/`522fa371e41` feature-flag reshuffle in
    `components/servo/Cargo.toml` → resolved in patches 25 and 30.
  - `5c2a3839525` clipboard backend → patch 25 partially superseded (see
    conflicts). `7bea9ed91f2` `Cow` origins → mirrored in patch 30.
    `fb04f8f1613` disk cache → patch 30 size rationale stale (see above).
  - `b8819327242` quick_cache, `e0b630fe3e0` aws-lc-rs SRI, `27a317dd69c`
    disk-cache temporary storage → `components/net/Cargo.toml` churn with
    no storage interaction.
  - keyword scan: `cb9b0804bda`/`748bf81026f` keyCode changes and
    `f9b4d1e6094` insertLineBreak are script-side (page-visible, not port);
    `c65518f8960`/`2a5f73586c3` CookieStoreManager is the DOM layer, not
    the NWeb cookie manager; `b3ee31d291c` media ohos dummy backend,
    `eff9ff0bfa4` OHOS font fallback, `e8ffe8e88ce` image decoder trait,
    `ba14b47a9a6` arboard wayland → not on the stack's surface.
- validation: check --ohos, tidy, fmt, test-unit servo-storage 53/53,
  arkweb release build + deploy, DAYU200 arkweb suite 30 passed / 1 xfailed,
  device nextest servo-storage sqlite 53/53 and RDB 43/43 (+1 documented
  skip), clipboard round-trip with/without READ_PASTEBOARD, servoshell
  release with and without `ohos-rdb-backend`, RDB-vs-sqlite same-HAP pref
  A/B on localhost origin, and the full WPT subset A/B via
  `./mach test-wpt --ohos` (runner from `wip-wpt-ohos` + device fixes on
  scratch branch `ohos-sync-2026-08-29-wpt`): 78 tests, 0 differing
  subtests, store proof both legs. `--flavor=harmonyos` does not install on
  the DAYU200 (profile UDID); default flavor, hap-sign-tool signing.
- found during validation: servoshell race `webdriver.rs:171 Expected at
  least one window to be open` when WebDriver answers before
  `on_surface_created`; `hdc file send -b` fails on the DAYU200 (hdc
  3.2.0b) — worked around in the runner, servoshell fix pending.
