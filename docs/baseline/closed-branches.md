# 關閉的分支（2026-10-01）

重構開始前，`main` 和 `refactor/process-planes` 以外的分支全部關閉。這份清單記下每個分支被刪除前的 head，需要時可以還原。

- **完全在 main 裡的分支**（「領先」為 0）：刪除不會遺失任何 commit。
- **有領先 commit 的分支：** 除了 `fix/bitget-idless-subscribe-ack`，head 都是某個 PR 的 head，GitHub 會在 `refs/pull/<n>/head` 保留這些 commit，也可以在 PR 頁面按 *Restore branch* 還原。
- **`fix/bitget-idless-subscribe-ack` @ `5012249`** 是 PR #65 合併之後才加上的一個 commit（只改 `uv.lock`，記錄 0.6.3 的 lock），沒有 PR 保留它。要保留的話，刪除前先執行 `git push origin 50122496db38313dc91c56131064097436576b52:refs/tags/archive/fix/bitget-idless-subscribe-ack`。

刪除指令（在有推送權限的本機 clone 執行）：

```sh
git fetch origin --prune
git ls-remote --heads origin | awk '{print $2}' | sed 's#refs/heads/##' \
  | grep -vxE 'main|refactor/process-planes' \
  | xargs git push origin --delete
```

| 分支 | head | 領先 / 落後 main | PR |
|---|---|---|---|
| `broker-communication-patterns` | `39482b8` | 0 / 106 | #85（merged） |
| `broker-provisioning` | `4c5f022` | 0 / 103 | #87（merged） |
| `broker-stream-shape` | `7c40da5` | 1 / 102 | #89（merged） |
| `cursor/broker-strategy-pattern-nats-2c02` | `7eae6ba` | 15 / 130 | #80（merged） |
| `cursor/deribit-book-frame-limit-b07b` | `418e55c` | 0 / 41 | #142（merged） |
| `cursor/deribit-reconnect-setup-7d3a` | `a46fdfd` | 0 / 13 | #150（merged） |
| `cursor/deribit-setup-context-0049` | `c453b9e` | 0 / 43 | #138（merged） |
| `cursor/dev-environment-setup-f1a2` | `2c73664` | 12 / 92 | #106（closed） |
| `cursor/docs-export-breathe-slice-deadline-83b6` | `affaac7` | 0 / 19 | #147（merged） |
| `cursor/jetstream-removal-impl-f3ab` | `88dc12a` | 14 / 92 | #105（merged） |
| `cursor/logviewer-rest-seed-4de5` | `6c16889` | 3 / 95 | #99（merged） |
| `cursor/md-print-liveness-3039` | `0754319` | 1 / 86 | #109（merged） |
| `cursor/native-broker-patterns-28c7` | `95c02d9` | 7 / 114 | #88（merged） |
| `cursor/nats-tape-ttl-kv-scan-consumers-2c02` | `94188b4` | 2 / 128 | #83（merged） |
| `cursor/nats-teardown-orphan-task-2c02` | `bfafb07` | 2 / 129 | #82（merged） |
| `cursor/oms-wait-cids-28f4` | `3659355` | 2 / 86 | #110（merged） |
| `cursor/remove-redis-transport-7fde` | `024523d` | 5 / 125 | #86（merged） |
| `cursor/strategy-identifier-f356` | `5e556b9` | 0 / 33 | #144（merged） |
| `cursor/sts-env-fanout-465d` | `9487e3b` | 3 / 94 | #100（merged） |
| `cursor/sts-registry-sync-ca06` | `81b270d` | 0 / 1 | #152（merged） |
| `cursor/yml-editor-td-hints-508a` | `efbc4ae` | 3 / 254 | #46（closed） |
| `feat/binance-future-dated` | `caa6378` | 0 / 202 | #60（merged） |
| `feat/binance-um-cm-rename` | `d78d2e6` | 0 / 197 | #61（merged） |
| `feat/deribit-option-md-greeks` | `0dbbe78` | 0 / 68 | #122（merged） |
| `feat/deribit-option-symbol-plane` | `77740b9` | 3 / 74 | #119（merged） |
| `feat/home-declare-annotate-retire` | `df90d05` | 0 / 140 | #76（merged） |
| `feat/in-app-update-md-overlap` | `b60a0b7` | 2 / 447 | #1（closed） |
| `feat/instances-1-td` | `33b4621` | 0 / 159 | #69（merged） |
| `feat/instances-2-sts` | `a77401f` | 0 / 158 | #70（merged） |
| `feat/instances-3-md` | `d3deb35` | 0 / 155 | #71（merged） |
| `feat/keys-page-shows-td-instance` | `1a5e6db` | 0 / 144 | #75（merged） |
| `feat/last-reader-unsubscribe` | `297dc6a` | 0 / 45 | #137（merged） |
| `feat/md-feed-end` | `b55248d` | 0 / 22 | #139（merged） |
| `feat/md-instrument-expiry` | `732c128` | 0 / 50 | #135（merged） |
| `feat/plane-instances` | `d3deb35` | 0 / 155 | #68（closed） |
| `feat/sts-artifacts` | `65af9c3` | 3 / 72 | #121（merged） |
| `feat/sts-deploy-instance-picker` | `58c94aa` | 3 / 96 | #97（merged） |
| `feat/sts-session-processes` | `80ade3c` | 0 / 56 | #130（merged） |
| `fix/bitget-idless-subscribe-ack` | `5012249` | 1 / 180 | #65（merged） |
| `fix/deribit-open-orders-cross-currency` | `d02c36c` | 1 / 78 | #116（merged） |
| `fix/eventlog-across-instances` | `4bb629f` | 0 / 148 | #74（merged） |
| `fix/live-fanout-core-nats` | `ae0b259` | 3 / 96 | #98（merged） |
| `fix/paper-market-outruns-book` | `a9e8c35` | 0 / 179 | #66（merged） |
| `fix/rearm-acks-after-stall` | `4408920` | 0 / 65 | #127（merged） |
| `fix/skip-cancel-while-pending` | `70c6603` | 3 / 84 | #108（merged） |
| `fix/sts-serves-its-subject` | `5f5273b` | 0 / 150 | #73（merged） |
| `fix/sts-start-deadline` | `8ddfc23` | 4 / 6 | #153（open） |
| `fix/stuck-session-stop` | `ae60ff7` | 0 / 9 | #149（merged） |
| `fix/tag-derived-version` | `3ee893e` | 1 / 76 | #117（merged） |
| `fix/tape-read-yields-loop` | `574710a` | 0 / 70 | #124（merged） |
| `fix/venue-reject-code-swap` | `810eeff` | 0 / 16 | #148（merged） |
| `remove-redis-transport` | `8673d99` | 0 / 120 | #84（merged） |
